package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.DatabaseStateManager;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

import java.util.List;

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

    /**
     * Infers the source graph's schema. Required by targets that need a column schema up front,
     * such as parquet.
     */
    default GraphSchema scanSchema(CopyOptions copyOptions) {
        throw new IllegalArgumentException(
                "A parquet target requires a bolt/neo4j or parquet source.");
    }

    /**
     * Returns the CREATE statements reproducing the source database's constraints and indexes.
     * Required by the --copy-indexes option.
     */
    default List<String> readIndexAndConstraintDdl() {
        throw new IllegalArgumentException(
                "--copy-indexes is only valid when the source address uses a bolt/neo4j scheme.");
    }

    @Override
    default void close() {
    }
}
