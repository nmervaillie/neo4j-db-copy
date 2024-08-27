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
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

class DataCopy {

	protected static final Logger LOG = LoggerFactory.getLogger(DataCopy.class);

	protected static final int WRITER_CONCURRENCY = 4;
	protected static final int BATCH_SIZE = 1000;

	private final Driver sourceDriver;
	private final String sourceDbName;
	private final Driver targetDriver;
	private final String targetDbName;

	public DataCopy(Driver sourceDriver, String sourceDbName, Driver targetDriver, String targetDbName) {
		this.sourceDriver = sourceDriver;
		this.sourceDbName = sourceDbName;
		this.targetDriver = targetDriver;
		this.targetDbName = targetDbName;
	}

	Mono<Long> copyAllNodesAndRels() {

		return readNodes()
				.buffer(BATCH_SIZE)
				.flatMap(this::writeNodes, WRITER_CONCURRENCY)
				.collectList()
				.flatMap(mapping -> readRels()
					.buffer(BATCH_SIZE)
					.flatMap((List<Relationship> relationships) -> writeRels(relationships, mapping), WRITER_CONCURRENCY)
					.reduce(0L, Long::sum)
				);
	}

	private Flux<Node> readNodes() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.executeRead(tx -> Flux.from(tx.run("MATCH (n) RETURN n")).flatMap(ReactiveResult::records)
						.map(record -> record.get(0).asNode())
						.doOnSubscribe(it -> LOG.info("Start reading nodes")))
				, ReactiveSession::close)
			.doOnComplete(() -> LOG.info("Nodes reading complete"));
	}

	private Flux<Relationship> readRels() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.executeRead(tx -> Mono.from(tx.run("MATCH ()-[rel]->() RETURN rel")).flatMapMany(ReactiveResult::records)
						.map(record -> record.get(0).asRelationship())
						.doOnSubscribe(it -> LOG.info("Start reading relationships")))
				, ReactiveSession::close)
			.doOnComplete(() -> LOG.info("Relationship reading complete"));
	}

	@SuppressWarnings("deprecation")
	private Flux<Mapping> writeNodes(List<Node> nodes) {
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
				session -> session.executeWrite(tx -> {
					List<Map<String, Object>> nodeData = nodes.stream()
							.map(node -> Map.of("s", node.id(), "l", labels(node), "p", node.asMap()))
							.toList();
					return Mono.from(tx.run("""
									UNWIND $inputList as input
									CREATE (n) SET n = input.p
									WITH n, input.s as sourceNodeId, input.l as labels
									CALL apoc.create.addLabels(n, labels) YIELD node
									RETURN sourceNodeId, id(n) as targetNodeId""",
							parameters("inputList", nodeData))).flatMapMany(ReactiveResult::records);
				})
				, ReactiveSession::close)
//				.doOnNext(it -> logBatchWrite())
				.map(r -> new Mapping(r.get("sourceNodeId").asLong(), r.get("targetNodeId").asLong()));
	}

    @SuppressWarnings("deprecation")
    private Mono<Long> writeRels(List<Relationship> relationships, List<Mapping> sourceToTargetNodeIdMapping) {

		var relData = relationships.stream()
				.map(rel -> Map.of(
						"s", findMapping(sourceToTargetNodeIdMapping, rel.startNodeId()),
						"t", findMapping(sourceToTargetNodeIdMapping, rel.endNodeId()),
						"type", rel.type(),
						"properties", rel.asMap()))
				.toList();
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
						session -> session.executeWrite(tx -> Mono.from(tx.run("""
								UNWIND $inputList as input
								MATCH (sourceNode) WHERE id(sourceNode)=input.s
								MATCH (targetNode) WHERE id(targetNode)=input.t
								CALL apoc.create.relationship(sourceNode, input.type, input.properties, targetNode) YIELD rel
								RETURN count(*)""",
								parameters("inputList", relData))).flatMapMany(ReactiveResult::records))
						, ReactiveSession::close)
//				.doOnNext(it -> logBatchWrite())
				.doOnComplete(() -> LOG.info("Relationships writing complete"))
				.map(record -> record.get(0).asLong())
				.reduce(0L, Long::sum);
	}

	private long findMapping(List<Mapping> mappings, long id) {
		 for (Mapping mapping : mappings) {
			 if (mapping.sourceNodeId == id) {
				 return mapping.targetNodeId;
			 }
		 }
		 throw new IllegalStateException("Unable to find source node with id " + id);
	}

	record Mapping(long sourceNodeId, long targetNodeId){}

	private static List<String> labels(Node node) {
		return StreamSupport.stream(node.labels().spliterator(), false).toList();
	}

	public Supplier<ReactiveSession> getSourceRxSession() {
		return () -> sourceDriver.session(ReactiveSession.class, SessionConfig.forDatabase(sourceDbName));
	}

	protected Supplier<ReactiveSession> getTargetRxSession() {
		return () -> targetDriver.session(ReactiveSession.class, SessionConfig.forDatabase(targetDbName));
	}

}
