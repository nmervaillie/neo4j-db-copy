package org.neo4j.dbcopy;

public interface DatabaseStateManager {

    default void makeReadOnly() {
    }

    default void restoreInitialState() {
    }
}
