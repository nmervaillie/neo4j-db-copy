package org.neo4j.dbcopy;

import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

class DataTransfer {

	protected static final Logger LOG = LoggerFactory.getLogger(DataTransfer.class);

	protected static final int WRITER_CONCURRENCY = 4;
	protected static final int BATCH_SIZE = 1000;

	private final Driver sourceDriver;
	private final String sourceDbName;
	private final Driver targetDriver;
	private final String targetDbName;
	private final CopyOptions copyOptions;

	public DataTransfer(Driver sourceDriver, String sourceDbName, Driver targetDriver, String targetDbName, CopyOptions copyOptions) {
		this.sourceDriver = sourceDriver;
		this.sourceDbName = sourceDbName;
		this.targetDriver = targetDriver;
		this.targetDbName = targetDbName;
		this.copyOptions = copyOptions;
	}

	Mono<Long> copyAllNodesAndRels() {
		var mappingContext = new MappingContext(10000);

        ProgressBar nodeProgressBar = new ProgressBar("Nodes", getTotalNodeCount());
		ProgressBar relationshipProgressBar = new ProgressBar("Relationships", getTotalRelationshipCount());

		return readNodes()
				.buffer(BATCH_SIZE)
				.doOnNext(batch -> nodeProgressBar.updateProgress(batch.size()))
				.flatMap(this::writeNodes, WRITER_CONCURRENCY)
				.collectList()
				.map(mappingContext::add)
				.flatMap(mappings -> readRels()
					.buffer(BATCH_SIZE)
					.doOnNext(batch -> relationshipProgressBar.updateProgress(batch.size()))
					.flatMap((List<Relationship> relationships) -> writeRels(relationships, mappings), WRITER_CONCURRENCY)
					.reduce(0L, Long::sum)
				)
				.doOnSuccess(it -> LOG.info("Relationships writing complete - {} relationships written", it));
	}

	private Flux<Node> readNodes() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.executeRead(tx -> Flux.from(tx.run("MATCH (n) RETURN n")).flatMap(ReactiveResult::records)
						.map(record -> record.get(0).asNode())
						.doOnSubscribe(it -> LOG.info("Start reading nodes")))
				, ReactiveSession::close);
	}

	private Flux<Relationship> readRels() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
						session -> session.executeRead(tx -> Mono.from(tx.run("MATCH ()-[rel]->() RETURN rel")).flatMapMany(ReactiveResult::records)
								.map(record -> record.get(0).asRelationship())
								.doOnSubscribe(it -> LOG.info("Start reading relationships")))
						, ReactiveSession::close);
	}

	private Flux<MappingContext.Mapping> writeNodes(List<Node> nodes) {
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
						session -> session.executeWrite(tx -> {
							List<Map<String, Object>> nodeData = nodes.stream()
									.map(node -> Map.of(
											"s", node.id(),
											"l", labels(node),
											"p", filterProperties(node.asMap(), copyOptions.getNodePropertiesToExclude())
									))
									.toList();
							return Mono.from(tx.run("""
                                    UNWIND $inputList as input
                                    CREATE (n) SET n = input.p
                                    WITH n, input.s as sourceNodeId, input.l as labels
                                    CALL apoc.create.addLabels(n, labels) YIELD node
                                    RETURN sourceNodeId, id(n) as targetNodeId""",
									parameters("inputList", nodeData))).flatMapMany(ReactiveResult::records);
						}),
						ReactiveSession::close)
				.map(r -> new MappingContext.Mapping(r.get("sourceNodeId").asLong(), r.get("targetNodeId").asLong()));
	}

    @SuppressWarnings("deprecation")
	private Mono<Long> writeRels(List<Relationship> relationships, MappingContext mappingContext) {
		var relData = relationships.stream()
				.map(rel -> Map.of(
						"s", mappingContext.get(rel.startNodeId()),
						"t", mappingContext.get(rel.endNodeId()),
						"type", rel.type(),
						"properties", filterProperties(rel.asMap(), copyOptions.getRelationshipPropertiesToExclude())))
				.toList();
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
						session -> session.executeWrite(tx -> Mono.from(tx.run("""
                        UNWIND $inputList as input
                        MATCH (sourceNode) WHERE id(sourceNode)=input.s
                        MATCH (targetNode) WHERE id(targetNode)=input.t
                        CALL apoc.create.relationship(sourceNode, input.type, input.properties, targetNode) YIELD rel
                        RETURN count(*)""",
								parameters("inputList", relData))).flatMapMany(ReactiveResult::records)),
						ReactiveSession::close)
				.map(record -> record.get(0).asLong())
				.reduce(0L, Long::sum);
	}

	private static List<String> labels(Node node) {
		return StreamSupport.stream(node.labels().spliterator(), false).toList();
	}

	private Map<String, Object> filterProperties(Map<String, Object> properties, Set<String> propertiesToExclude) {
		return properties.entrySet().stream()
				.filter(entry -> !propertiesToExclude.contains(entry.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public Supplier<ReactiveSession> getSourceRxSession() {
		return () -> sourceDriver.session(ReactiveSession.class, SessionConfig.forDatabase(sourceDbName));
	}

	protected Supplier<ReactiveSession> getTargetRxSession() {
		return () -> targetDriver.session(ReactiveSession.class, SessionConfig.forDatabase(targetDbName));
	}

	private long getTotalNodeCount() {
		try (var session = sourceDriver.session(SessionConfig.forDatabase(sourceDbName))) {
			return session.run("MATCH (n) RETURN count(n) AS count").single().get("count").asLong();
		}
	}

	private long getTotalRelationshipCount() {
		try (var session = sourceDriver.session(SessionConfig.forDatabase(sourceDbName))) {
			return session.run("MATCH ()-[r]->() RETURN count(r) AS count").single().get("count").asLong();
		}
	}
}
