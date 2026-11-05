package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

import java.util.List;


public interface DataWriter extends AutoCloseable {

    /**
     * returns a flux of updated node count
     */
    Flux<Long> writeNodes(Flux<Node> nodes, CopyOptions copyOptions);

    /**
     * returns a flux of updated relationships count
     */
    Flux<Long> writeRelationships(Flux<Relationship> relationships, CopyOptions copyOptions);

    /**
     * Applies the given constraint/index CREATE statements, once the data copy is complete.
     * Required by the --copy-indexes option.
     */
    default void applyIndexAndConstraintDdl(List<String> ddlStatements) {
        throw new IllegalArgumentException(
                "--copy-indexes is only valid when the target address uses a bolt/neo4j scheme.");
    }

    @Override
    default void close() throws Exception {
    }
}
