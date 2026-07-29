package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.bolt.BoltWriter;
import org.neo4j.dbcopy.json.JsonDataWriter;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.neo4j.driver.Values.parameters;

public final class DataWriterFactory extends EndpointFactorySupport {

    private static final Logger LOG = LoggerFactory.getLogger(DataWriterFactory.class);

    public DataWriter createWriter(URI targetAddress, String targetUserName, String targetPassword, String targetDatabase,
                                   boolean createTargetDatabase) {
        EndpointKind kind = endpointKind(targetAddress);
        return switch (kind) {
            case BOLT ->
                    createBoltWriter(targetAddress, targetUserName, targetPassword, targetDatabase, createTargetDatabase);
            case JSON -> new JsonDataWriter(toJsonFilePath(targetAddress, "target"));
        };
    }

    private DataWriter createBoltWriter(URI targetAddress, String targetUserName, String targetPassword, String targetDatabase,
                                        boolean createTargetDatabase) {
        requireOption("--target-password", targetPassword);
        requireOption("--target-database", targetDatabase);

        Driver driver = GraphDatabase.driver(targetAddress, AuthTokens.basic(targetUserName, targetPassword));
        try {
            if (createTargetDatabase) {
                createDatabaseIfNotExists(driver, targetDatabase);
            }
            return new BoltWriter(driver, targetDatabase);
        } catch (RuntimeException e) {
            closeQuietly(driver);
            throw userFriendlyBoltException(targetAddress, targetDatabase, e);
        }
    }

    private void createDatabaseIfNotExists(Driver driver, String targetDatabase) {
        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            if (databaseExists(targetDatabase, session)) {
                return;
            }
            LOG.info("Database '{}' does not exist, creating it", targetDatabase);
            session.run("CREATE DATABASE " + quoteIdentifier(targetDatabase) + " IF NOT EXISTS WAIT").consume();
        }
    }

    private static boolean databaseExists(String targetDatabase, Session session) {
        return !session.run("SHOW DATABASES YIELD name WHERE name = $name", parameters("name", targetDatabase))
                .list().isEmpty();
    }

    private static String quoteIdentifier(String name) {
        return "`" + name.replace("`", "``") + "`";
    }
}
