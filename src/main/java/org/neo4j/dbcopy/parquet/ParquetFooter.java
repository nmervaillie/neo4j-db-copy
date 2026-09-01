package org.neo4j.dbcopy.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * The {@code neo4j.*} keys stamped into each parquet file's footer. They record the declared Neo4j
 * type of every property, so import never has to infer a type from the physical parquet layout.
 */
public record ParquetFooter(String recordType,
                            List<String> labels,
                            String relationshipType,
                            SortedMap<String, PropertyType> types) {

    public static final String VERSION = "1";
    public static final String RECORD_TYPE_NODE = "node";
    public static final String RECORD_TYPE_RELATIONSHIP = "relationship";

    private static final String KEY_VERSION = "neo4j.dbcopy.version";
    private static final String KEY_RECORD_TYPE = "neo4j.recordType";
    private static final String KEY_LABELS = "neo4j.labels";
    private static final String KEY_RELATIONSHIP_TYPE = "neo4j.relationshipType";
    private static final String KEY_TYPES = "neo4j.types";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ParquetFooter forNode(SortedSet<String> labels,
                                        SortedMap<String, PropertyType> types) {
        return new ParquetFooter(RECORD_TYPE_NODE, List.copyOf(labels), null, types);
    }

    public static ParquetFooter forRelationship(String relationshipType,
                                                SortedMap<String, PropertyType> types) {
        return new ParquetFooter(RECORD_TYPE_RELATIONSHIP, List.of(), relationshipType, types);
    }

    public Map<String, String> toKeyValueMetadata() {
        Map<String, String> tokens = new LinkedHashMap<>();
        types.forEach((name, type) -> tokens.put(name, Neo4jTypeInference.toToken(type)));

        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(KEY_VERSION, VERSION);
        metadata.put(KEY_RECORD_TYPE, recordType);
        if (RECORD_TYPE_NODE.equals(recordType)) {
            metadata.put(KEY_LABELS, writeJson(labels));
        } else {
            metadata.put(KEY_RELATIONSHIP_TYPE, relationshipType);
        }
        metadata.put(KEY_TYPES, writeJson(tokens));
        return metadata;
    }

    public static ParquetFooter fromKeyValueMetadata(Map<String, String> metadata, Path file) {
        String version = metadata.get(KEY_VERSION);
        if (version == null) {
            throw new IllegalArgumentException(file + " is not a neo4j-db-copy parquet export: "
                    + "its footer has no '" + KEY_VERSION + "' entry.");
        }
        if (!VERSION.equals(version)) {
            throw new IllegalArgumentException(file + " was written by an incompatible version of "
                    + "neo4j-db-copy (footer version " + version + ", this build reads version "
                    + VERSION + ").");
        }

        String recordType = metadata.get(KEY_RECORD_TYPE);
        SortedMap<String, PropertyType> types = readTypes(metadata.get(KEY_TYPES), file);

        if (RECORD_TYPE_NODE.equals(recordType)) {
            List<String> labels = readJson(metadata.get(KEY_LABELS), new TypeReference<>() {
            }, file);
            return new ParquetFooter(RECORD_TYPE_NODE, labels, null, types);
        }
        if (RECORD_TYPE_RELATIONSHIP.equals(recordType)) {
            String relationshipType = metadata.get(KEY_RELATIONSHIP_TYPE);
            if (relationshipType == null) {
                throw new IllegalArgumentException("Incomplete neo4j-db-copy footer in " + file);
            }
            return new ParquetFooter(RECORD_TYPE_RELATIONSHIP, List.of(), relationshipType, types);
        }
        throw new IllegalArgumentException(
                "Unsupported '" + KEY_RECORD_TYPE + "' value '" + recordType + "' in " + file);
    }

    private static SortedMap<String, PropertyType> readTypes(String json, Path file) {
        Map<String, String> tokens = readJson(json, new TypeReference<>() {
        }, file);
        SortedMap<String, PropertyType> types = new TreeMap<>();
        tokens.forEach((name, token) -> types.put(name, Neo4jTypeInference.fromToken(token)));
        return types;
    }

    private static String writeJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to serialise parquet footer metadata", e);
        }
    }

    private static <T> T readJson(String json, TypeReference<T> type, Path file) {
        if (json == null) {
            throw new IllegalArgumentException("Incomplete neo4j-db-copy footer in " + file);
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unreadable neo4j-db-copy footer in " + file, e);
        }
    }
}
