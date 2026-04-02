package org.neo4j.dbcopy.io;

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
}
