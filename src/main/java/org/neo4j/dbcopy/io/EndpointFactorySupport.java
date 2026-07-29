package org.neo4j.dbcopy.io;

import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;

abstract class EndpointFactorySupport {

    protected enum EndpointKind {
        BOLT,
        JSON
    }

    protected EndpointKind endpointKind(URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("Address must have a scheme: " + uri);
        }
        String lower = scheme.toLowerCase();
        if (lower.startsWith("bolt") || lower.startsWith("neo4j")) {
            return EndpointKind.BOLT;
        }
        if ("file".equals(lower)) {
            return EndpointKind.JSON;
        }
        throw new IllegalArgumentException("Unsupported scheme '" + scheme + "' in address: " + uri +
                ". Supported schemes are bolt/neo4j and file.");
    }

    protected void requireOption(String optionName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(optionName + " is required when using a bolt/neo4j endpoint.");
        }
    }

    protected Path toJsonFilePath(URI uri, String endpointName) {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("The " + endpointName + " JSON endpoint must use a file URI: " + uri);
        }
        try {
            Path path = Path.of(uri);
            if (Objects.toString(path, "").isBlank()) {
                throw new IllegalArgumentException("The " + endpointName + " JSON endpoint must point to a file: " + uri);
            }
            return path;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid " + endpointName + " file URI '" + uri + "': " + e.getMessage(), e);
        }
    }

    protected IllegalArgumentException userFriendlyBoltException(URI address, String database, RuntimeException cause) {
        if (cause instanceof ServiceUnavailableException) {
            return new IllegalArgumentException(
                    "Unable to connect to the Neo4j server at " + address +
                            ". Check that the server is running and the address is correct.",
                    cause);
        }
        if (cause instanceof Neo4jException authException && "Neo.ClientError.Security.Unauthorized".equals(authException.code())) {
            return new IllegalArgumentException(
                    "Unable to authenticate to the Neo4j server at " + address +
                            ". Check that the username and password are correct.",
                    cause);
        }
        if (cause instanceof Neo4jException neo4jException && "Neo.ClientError.Database.DatabaseNotFound".equals(neo4jException.code())) {
            return new IllegalArgumentException(
                    "Unable to open database '" + database + "' at " + address +
                            ". Check that the database exists and that your user can access it.",
                    cause);
        }
        if (cause instanceof Neo4jException adminException && "Neo.ClientError.Statement.UnsupportedAdministrationCommand".equals(adminException.code())) {
            return new IllegalArgumentException("Creating databases requires Neo4j Enterprise Edition.", cause);
        }
        return new IllegalArgumentException(
                "Unable to open Neo4j endpoint at " + address + ": " + cause.getMessage(),
                cause);
    }

    protected void closeQuietly(Driver driver) {
        try {
            driver.close();
        } catch (Exception ignored) {
        }
    }
}
