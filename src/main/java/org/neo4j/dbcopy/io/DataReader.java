package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.DatabaseStateManager;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

public interface DataReader extends AutoCloseable {

    Flux<Node> readNodes();

    Flux<Relationship> readRelationships();

    long getTotalNodeCount();

    long getTotalRelationshipCount();

    default DatabaseStateManager createStateManager(boolean lockSourceDatabase) {
        if (lockSourceDatabase) {
            throw new IllegalArgumentException("--lock-source-database is only valid when the source address uses a bolt/neo4j scheme.");
        }
        return new DatabaseStateManager() {
        };
    }

    @Override
    default void close() {
    }
}
