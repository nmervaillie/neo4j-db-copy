package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;


public interface DataWriter extends AutoCloseable {

    /**
     * returns a flux of updated node count
     */
    Flux<Long> writeNodes(Flux<Node> nodes, CopyOptions copyOptions);

    /**
     * returns a flux of updated relationships count
     */
    Flux<Long> writeRelationships(Flux<Relationship> relationships, CopyOptions copyOptions);

    @Override
    default void close() throws Exception {
    }
}
