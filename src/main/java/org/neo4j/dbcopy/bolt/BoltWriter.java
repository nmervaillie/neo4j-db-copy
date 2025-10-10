package org.neo4j.dbcopy.bolt;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.DataWriter;
import org.neo4j.dbcopy.MappingContext;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

public class BoltWriter implements DataWriter {

    private final Driver targetDriver;
    private final String targetDatabase;

    public BoltWriter(Driver targetDriver, String targetDatabase) {
        this.targetDriver = targetDriver;
        this.targetDatabase = targetDatabase;
    }

    @Override
    public Flux<MappingContext.Mapping> writeNodes(List<Node> nodes, CopyOptions copyOptions) {
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
                .map(r -> new MappingContext.Mapping(r.get("sourceNodeId").asLong(), r.get("targetNodeId").asLong()));
    }

    @Override
    @SuppressWarnings("deprecation")
    public Mono<Long> writeRelationships(List<Relationship> relationships, MappingContext mappingContext, CopyOptions copyOptions) {
        var relData = relationships.stream()
                .map(rel -> Map.of(
                        "s", mappingContext.get(rel.startNodeId()),
                        "t", mappingContext.get(rel.endNodeId()),
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
                .reduce(0L, Long::sum);
    }

    protected Supplier<ReactiveSession> getRxSession() {
        return () -> targetDriver.session(ReactiveSession.class, SessionConfig.forDatabase(targetDatabase));
    }

    private static List<String> labels(Node node) {
        return StreamSupport.stream(node.labels().spliterator(), false).toList();
    }

    private Map<String, Object> filterProperties(Map<String, Object> properties, Set<String> propertiesToExclude) {
        return properties.entrySet().stream()
                .filter(entry -> !propertiesToExclude.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
