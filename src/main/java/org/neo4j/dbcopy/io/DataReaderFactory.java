package org.neo4j.dbcopy.io;

import org.neo4j.dbcopy.bolt.BoltReader;
import org.neo4j.dbcopy.json.JsonDataReader;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.net.URI;

public final class DataReaderFactory extends EndpointFactorySupport {

    public DataReader createReader(URI sourceAddress, String sourceUserName, String sourcePassword, String sourceDatabase) {
        EndpointKind kind = endpointKind(sourceAddress);
        return switch (kind) {
            case BOLT -> createBoltReader(sourceAddress, sourceUserName, sourcePassword, sourceDatabase);
            case JSON -> new JsonDataReader(toJsonFilePath(sourceAddress, "source"));
        };
    }

    private DataReader createBoltReader(URI sourceAddress, String sourceUserName, String sourcePassword, String sourceDatabase) {
        requireOption("--source-password", sourcePassword);
        requireOption("--source-database", sourceDatabase);

        Config config = Config.builder()
                .withFetchSize(5000)
                .build();
        Driver driver = GraphDatabase.driver(sourceAddress, AuthTokens.basic(sourceUserName, sourcePassword), config);
        try {
            return new BoltReader(driver, sourceDatabase);
        } catch (RuntimeException e) {
            closeQuietly(driver);
            throw userFriendlyBoltException(sourceAddress, sourceDatabase, e);
        }
    }
}
