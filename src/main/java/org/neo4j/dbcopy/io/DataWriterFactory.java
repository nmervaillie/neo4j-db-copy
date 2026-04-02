package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.bolt.BoltWriter;
import org.neo4j.dbcopy.json.JsonDataWriter;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.net.URI;

public final class DataWriterFactory extends EndpointFactorySupport {

    public DataWriter createWriter(URI targetAddress, String targetUserName, String targetPassword, String targetDatabase) {
        EndpointKind kind = endpointKind(targetAddress);
        return switch (kind) {
            case BOLT -> createBoltWriter(targetAddress, targetUserName, targetPassword, targetDatabase);
            case JSON -> new JsonDataWriter(toJsonFilePath(targetAddress, "target"));
        };
    }

    private DataWriter createBoltWriter(URI targetAddress, String targetUserName, String targetPassword, String targetDatabase) {
        requireOption("--target-password", targetPassword);
        requireOption("--target-database", targetDatabase);

        Driver driver = GraphDatabase.driver(targetAddress, AuthTokens.basic(targetUserName, targetPassword));
        driver.verifyConnectivity();
        return new BoltWriter(driver, targetDatabase);
    }
}
