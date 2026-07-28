package org.neo4j.dbcopy.bolt;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.io.DataWriter;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

public class BoltWriter implements DataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BoltWriter.class);
    private static final int NODE_WRITE_CONCURRENCY = 4;

    private final Driver targetDriver;
    private final String targetDatabase;
    private final Map<Long, Long> nodeIdMappings = new ConcurrentHashMap<>(10000);

    public BoltWriter(Driver targetDriver, String targetDatabase) {
        this.targetDriver = targetDriver;
        this.targetDatabase = targetDatabase;
        verifyDatabaseAccessible();
        LOG.info("Connected to target database '{}'", targetDatabase);
    }

    @Override
    public Flux<Long> writeNodes(Flux<Node> nodes, CopyOptions copyOptions) {
        return nodes.buffer(copyOptions.batchSize())
                .flatMap(batch -> writeNodeBatch(batch, copyOptions), NODE_WRITE_CONCURRENCY);
    }

    @Override
    public Flux<Long> writeRelationships(Flux<Relationship> relationships, CopyOptions copyOptions) {
        return relationships.buffer(copyOptions.batchSize())
                .concatMap(batch -> writeRelationshipBatch(batch, copyOptions));
    }

    @SuppressWarnings("deprecation")
    private Mono<Long> writeNodeBatch(List<Node> nodes, CopyOptions copyOptions) {
        if (nodes.isEmpty()) {
            return Mono.empty();
        }
        return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
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
                .doOnNext(record -> nodeIdMappings.put(record.get("sourceNodeId").asLong(), record.get("targetNodeId").asLong()))
                .count();
    }

    @SuppressWarnings("deprecation")
    private Mono<Long> writeRelationshipBatch(List<Relationship> relationships, CopyOptions copyOptions) {
        if (relationships.isEmpty()) {
            return Mono.empty();
        }
        var relData = relationships.stream()
                .map(rel -> Map.of(
                        "s", mappedNodeId(rel.startNodeId()),
                        "t", mappedNodeId(rel.endNodeId()),
                        "type", rel.type(),
                        "properties", filterProperties(rel.asMap(), copyOptions.getRelationshipPropertiesToExclude())))
                .toList();
        return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
                        session -> session.executeWrite(tx -> Mono.from(tx.run("""
                        UNWIND $inputList as input
                        MATCH (sourceNode) WHERE id(sourceNode)=input.s
                        MATCH (targetNode) WHERE id(targetNode)=input.t
                        CALL apoc.create.relationship(sourceNode, input.type, input.properties, targetNode) YIELD rel
                        RETURN count(*)""",
                                parameters("inputList", relData))).flatMapMany(ReactiveResult::records)),
                        ReactiveSession::close)
                .map(record -> record.get(0).asLong())
                .single(0L);
    }

    private long mappedNodeId(long sourceNodeId) {
        Long targetNodeId = nodeIdMappings.get(sourceNodeId);
        if (targetNodeId == null) {
            throw new IllegalStateException("Unable to find source node with id " + sourceNodeId);
        }
        return targetNodeId;
    }

    protected Supplier<ReactiveSession> getRxSession() {
        return () -> targetDriver.session(ReactiveSession.class, SessionConfig.forDatabase(targetDatabase));
    }

    private void verifyDatabaseAccessible() {
        targetDriver.verifyConnectivity();
        try (var session = targetDriver.session(SessionConfig.forDatabase(targetDatabase))) {
            session.run("RETURN 1").consume();
        }
    }

    private static List<String> labels(Node node) {
        return StreamSupport.stream(node.labels().spliterator(), false).toList();
    }

    private Map<String, Object> filterProperties(Map<String, Object> properties, Set<String> propertiesToExclude) {
        return properties.entrySet().stream()
                .filter(entry -> !propertiesToExclude.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void close() {
        targetDriver.close();
    }
}
