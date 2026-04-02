package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Mono;

import java.util.List;

public interface DataWriter extends AutoCloseable {

    Mono<Void> writeNodes(List<Node> nodes, CopyOptions copyOptions);

    Mono<Long> writeRelationships(List<Relationship> relationships, CopyOptions copyOptions);

    @Override
    default void close() throws Exception {
    }
}
