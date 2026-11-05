package org.neo4j.dbcopy.bolt;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads the constraints and indexes of a Neo4j database and rebuilds equivalent CREATE
 * statements, so they can be replayed on the target database once the data copy is done.
 * Constraint-backed indexes are skipped: they are created by their own constraint.
 */
public final class BoltSchemaDdlReader {

    private final Driver driver;
    private final String database;

    public BoltSchemaDdlReader(Driver driver, String database) {
        this.driver = driver;
        this.database = database;
    }

    public List<String> readDdl() {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            List<String> ddl = new ArrayList<>();
            ddl.addAll(session.run("SHOW CONSTRAINTS YIELD createStatement")
                    .list(record -> withIfNotExists(record.get("createStatement").asString())));
            ddl.addAll(session.run("SHOW INDEXES YIELD createStatement, owningConstraint, type").list().stream()
                    .filter(record -> record.get("owningConstraint").isNull())
                    .filter(record -> !"LOOKUP".equals(record.get("type").asString()))
                    .map(record -> withIfNotExists(record.get("createStatement").asString()))
                    .toList());
            return ddl;
        }
    }

    /**
     * The createStatement column returns statements such as
     * "CREATE CONSTRAINT my_name FOR (n:Person) REQUIRE n.name IS UNIQUE"; this appends
     * IF NOT EXISTS after the name so replaying is idempotent.
     */
    private static String withIfNotExists(String createStatement) {
        int forClause = createStatement.indexOf(" FOR ");
        if (forClause < 0) {
            throw new IllegalStateException("Unable to read the CREATE statement: " + createStatement);
        }
        return createStatement.substring(0, forClause) + " IF NOT EXISTS " + createStatement.substring(forClause);
    }
}
