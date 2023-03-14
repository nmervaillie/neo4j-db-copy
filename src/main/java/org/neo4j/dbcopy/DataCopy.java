package org.neo4j.dbcopy;

import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

class DataCopy {

	protected static final Logger LOG = LoggerFactory.getLogger(DataCopy.class);

	protected static final int WRITER_CONCURRENCY = 4;

	public DataCopy(Driver sourceDriver, String sourceDbName, Driver targetDriver, String targetDbName) {
		this.sourceDriver = sourceDriver;
		this.sourceDbName = sourceDbName;
		this.targetDriver = targetDriver;
		this.targetDbName = targetDbName;
	}

	private final Driver sourceDriver;
	private final String sourceDbName;
	private final Driver targetDriver;
	private final String targetDbName;

	protected static final int BATCH_SIZE = 1000;

	private List<Mapping> sourceToTargetNodeIdMapping = new ArrayList<>();

	void copyAllNodesAndRels() {

		sourceToTargetNodeIdMapping = readNodes()
				.buffer(BATCH_SIZE)
//				.doOnEach(it -> logBatchRead())
				.flatMap(this::writeNodes, WRITER_CONCURRENCY)
				.collectList()
//				.reduce(0L, (count, result) -> count + result.counters().nodesCreated())
				.block();

		Long nbRelsWritten = readRels()
				.buffer(BATCH_SIZE)
				.flatMap(this::writeRels, WRITER_CONCURRENCY)
				.reduce(0L, Long::sum)
				.block();
		System.out.println(nbRelsWritten + " rels written");
	}

	private Flux<Node> readNodes() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.readTransaction(tx -> Flux.from(tx.run("MATCH (n) RETURN n").records())
						.map(record -> record.get(0).asNode())
						.doOnSubscribe(it -> LOG.info("Start reading")))
				, RxSession::close)
			.doOnComplete(() -> LOG.info("\nReading complete"));
	}

	private Flux<Relationship> readRels() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.readTransaction(tx -> Flux.from(tx.run("MATCH ()-[rel]->() RETURN rel").records())
						.map(record -> record.get(0).asRelationship())
						.doOnSubscribe(it -> LOG.info("Start reading")))
				, RxSession::close)
			.doOnComplete(() -> LOG.info("\nReading rels complete"));
	}

	private Flux<Mapping> writeNodes(List<Node> nodes) {
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
				session -> session.writeTransaction(tx -> {
					List<Map<String, Object>> nodeData = nodes.stream()
							.map(node -> Map.of("s", node.id(), "l", labels(node), "p", node.asMap()))
							.toList();
					return tx.run("""
									UNWIND $inputList as input
									CREATE (n) SET n = input.p
									WITH n, input.s as sourceNodeId, input.l as labels
									CALL apoc.create.addLabels(n, labels) YIELD node
									RETURN sourceNodeId, id(n) as targetNodeId""",
							parameters("inputList", nodeData)).records();
				})
				, RxSession::close)
//				.doOnNext(it -> logBatchWrite())
				.map(r -> new Mapping(r.get("sourceNodeId").asLong(), r.get("targetNodeId").asLong()));
	}

	private Mono<Long> writeRels(List<Relationship> relationships) {

		var relData = relationships.stream()
				.map(rel -> Map.of(
						"s", findMapping(sourceToTargetNodeIdMapping, rel.startNodeId()),
						"t", findMapping(sourceToTargetNodeIdMapping, rel.endNodeId()),
						"type", rel.type(),
						"properties", rel.asMap()))
				.toList();
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
						session -> session.writeTransaction(tx -> tx.run("""
								UNWIND $inputList as input
								MATCH (sourceNode) WHERE id(sourceNode)=input.s
								MATCH (targetNode) WHERE id(targetNode)=input.t
								CALL apoc.create.relationship(sourceNode, input.type, input.properties, targetNode) YIELD rel
								RETURN count(*)""",
								parameters("inputList", relData)).records())
						, RxSession::close)
//				.doOnNext(it -> logBatchWrite())
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

	public Supplier<RxSession> getSourceRxSession() {
		return () -> sourceDriver.rxSession(SessionConfig.forDatabase(sourceDbName));
	}

	protected Supplier<RxSession> getTargetRxSession() {
		return () -> targetDriver.rxSession(SessionConfig.forDatabase(targetDbName));
	}

}
