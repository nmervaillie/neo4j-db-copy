package org.neo4j.dbcopy.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

public final class JsonDataReader implements DataReader {

    private final Path path;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonDataReader(Path path) {
        this.path = path;
    }

    @Override
    public Flux<Node> readNodes() {
        AtomicLong lineNumber = new AtomicLong();
        return Flux.using(
                () -> new BufferedReader(new InputStreamReader(JsonIo.newInputStream(path), StandardCharsets.UTF_8)),
                reader -> Flux.fromStream(reader.lines())
                        .filter(line -> !line.isBlank())
                        .map(line -> parseRoot(lineNumber.incrementAndGet(), line))
                        .filter(root -> "node".equals(recordType(root, lineNumber.get())))
                        .map(this::toNode),
                this::closeQuietly
        ).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Flux<Relationship> readRelationships() {
        AtomicLong lineNumber = new AtomicLong();
        return Flux.using(
                () -> new BufferedReader(new InputStreamReader(JsonIo.newInputStream(path), StandardCharsets.UTF_8)),
                reader -> Flux.fromStream(reader.lines())
                        .filter(line -> !line.isBlank())
                        .map(line -> parseRoot(lineNumber.incrementAndGet(), line))
                        .filter(root -> "relationship".equals(recordType(root, lineNumber.get())))
                        .map(this::toRelationship),
                this::closeQuietly
        ).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public long getTotalNodeCount() {
        return countByRecordType("node");
    }

    @Override
    public long getTotalRelationshipCount() {
        return countByRecordType("relationship");
    }

    private JsonNode parseRoot(long lineNumber, String line) {
        try {
            return objectMapper.readTree(line);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse JSONL line " + lineNumber + " in " + path, e);
        }
    }

    private long countByRecordType(String type) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(JsonIo.newInputStream(path), StandardCharsets.UTF_8));
             Stream<String> lines = reader.lines()) {
            AtomicLong lineNumber = new AtomicLong();
            return lines
                    .filter(line -> !line.isBlank())
                    .map(line -> parseRoot(lineNumber.incrementAndGet(), line))
                    .filter(root -> type.equals(recordType(root, lineNumber.get())))
                    .count();
        } catch (IOException e) {
            throw new RuntimeException("Failed to count records in " + path, e);
        }
    }

    private Node toNode(JsonNode root) {
        long id = requireLong(root, "id", "node");

        List<String> labels = new ArrayList<>();
        for (JsonNode l : requireArray(root, "labels", "node")) {
            labels.add(l.asText());
        }

        Map<String, Value> properties = parseProperties(requireObject(root, "properties", "node"));

        return new JsonNodeEntity(id, labels, properties);
    }

    private Relationship toRelationship(JsonNode root) {
        long id = requireLong(root, "id", "relationship");
        long startId = requireLong(root, "startId", "relationship");
        long endId = requireLong(root, "endId", "relationship");
        String type = requireText(root, "type", "relationship");

        Map<String, Value> properties = parseProperties(requireObject(root, "properties", "relationship"));

        return new JsonRelationshipEntity(id, startId, endId, type, properties);
    }

    private Map<String, Value> parseProperties(JsonNode propsNode) {
        Map<String, Value> props = new LinkedHashMap<>();
        if (propsNode != null && propsNode.isObject()) {
            for (Map.Entry<String, JsonNode> entry : propsNode.properties()) {
                String key = entry.getKey();
                JsonNode tv = entry.getValue();
                props.put(key, parseTypedValue(tv));
            }
        }
        return props;
    }

    private Value parseTypedValue(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("Expected a typed value object in " + path);
        }
        String type = requireText(node, "type", "typed value");

        return switch (type) {
            case "Null" -> Values.NULL;

            case "Boolean" -> Values.value(requireBooleanNode(node, "value", type).asBoolean());
            case "Integer" -> Values.value(requireIntegralNode(node, "value", type).asLong());
            case "Float"   -> Values.value(requireNumericNode(node, "value", type).asDouble());
            case "String"  -> Values.value(requireText(node, "value", type));

            case "ByteArray" -> {
                String base64 = requireText(node, "value", type);
                try {
                    yield Values.value(Base64.getDecoder().decode(base64));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid base64 byte array value in " + path, e);
                }
            }

            case "List" -> parseList(node.get("value"));

            case "LocalDate"      -> Values.value(LocalDate.parse(requireText(node, "value", type)));
            case "LocalTime"      -> Values.value(LocalTime.parse(requireText(node, "value", type)));
            case "OffsetTime"     -> Values.value(OffsetTime.parse(requireText(node, "value", type)));
            case "LocalDateTime"  -> Values.value(LocalDateTime.parse(requireText(node, "value", type)));
            case "OffsetDateTime" -> Values.value(OffsetDateTime.parse(requireText(node, "value", type),
                    DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            case "ZonedDateTime"  -> Values.value(ZonedDateTime.parse(requireText(node, "value", type),
                    DateTimeFormatter.ISO_ZONED_DATE_TIME));

            case "Duration" -> {
                JsonNode v = requireObject(node, "value", type);
                long months  = requireLong(v, "months", type);
                long days    = requireLong(v, "days", type);
                long seconds = requireLong(v, "seconds", type);
                int nanos    = requireIntegralNode(v, "nanos", type).asInt();
                yield Values.isoDuration(months, days, seconds, nanos);
            }

            case "Point" -> {
                int srid = requireIntegralNode(node, "srid", type).asInt();
                JsonNode v = requireObject(node, "value", type);

                double x = requireNumericNode(v, "x", type).asDouble();
                double y = requireNumericNode(v, "y", type).asDouble();
                JsonNode zNode = v.get("z");

                if (zNode == null || zNode.isNull()) {
                    yield Values.point(srid, x, y);
                } else {
                    double z = zNode.asDouble();
                    yield Values.point(srid, x, y, z);
                }
            }

            default -> throw new IllegalArgumentException("Unsupported typed value '" + type + "' in " + path);
        };
    }

    private Value parseList(JsonNode valueNode) {
        if (valueNode == null || !valueNode.isArray()) {
            throw new IllegalArgumentException("List typed values must contain an array in " + path);
        }
        ArrayNode array = (ArrayNode) valueNode;
        List<Object> list = new ArrayList<>(array.size());
        for (JsonNode elementNode : array) {
            Value v = parseTypedValue(elementNode);
            list.add(v.asObject());
        }
        return Values.value(list);
    }

    private void closeQuietly(BufferedReader reader) {
        try {
            reader.close();
        } catch (IOException ignore) {
        }
    }

    private String recordType(JsonNode root, long lineNumber) {
        JsonNode recordTypeNode = root.get("recordType");
        if (recordTypeNode == null || !recordTypeNode.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid recordType on line " + lineNumber + " in " + path);
        }
        return recordTypeNode.asText();
    }

    private JsonNode requireObject(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.isObject()) {
            throw new IllegalArgumentException("Missing or invalid object field '" + fieldName + "' for " + context + " in " + path);
        }
        return field;
    }

    private ArrayNode requireArray(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.isArray()) {
            throw new IllegalArgumentException("Missing or invalid array field '" + fieldName + "' for " + context + " in " + path);
        }
        return (ArrayNode) field;
    }

    private String requireText(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.isTextual()) {
            throw new IllegalArgumentException("Missing or invalid text field '" + fieldName + "' for " + context + " in " + path);
        }
        return field.asText();
    }

    private long requireLong(JsonNode root, String fieldName, String context) {
        return requireIntegralNode(root, fieldName, context).asLong();
    }

    private JsonNode requireIntegralNode(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.canConvertToLong()) {
            throw new IllegalArgumentException("Missing or invalid integral field '" + fieldName + "' for " + context + " in " + path);
        }
        return field;
    }

    private JsonNode requireNumericNode(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.isNumber()) {
            throw new IllegalArgumentException("Missing or invalid numeric field '" + fieldName + "' for " + context + " in " + path);
        }
        return field;
    }

    private JsonNode requireBooleanNode(JsonNode root, String fieldName, String context) {
        JsonNode field = root.get(fieldName);
        if (field == null || !field.isBoolean()) {
            throw new IllegalArgumentException("Missing or invalid boolean field '" + fieldName + "' for " + context + " in " + path);
        }
        return field;
    }

    private static final class JsonNodeEntity implements Node {

        private final long id;
        private final List<String> labels;
        private final Map<String, Value> properties;

        JsonNodeEntity(long id, List<String> labels, Map<String, Value> properties) {
            this.id = id;
            this.labels = List.copyOf(labels);
            this.properties = Map.copyOf(properties);
        }

        @Override
        public String elementId() { // we don't care, this is unused
            return id + "";
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public Map<String, Object> asMap() {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<String, Value> e : properties.entrySet()) {
                result.put(e.getKey(), e.getValue().asObject());
            }
            return result;
        }

        @Override
        public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
            Map<String, T> result = new LinkedHashMap<>();
            for (Map.Entry<String, Value> e : properties.entrySet()) {
                result.put(e.getKey(), mapFunction.apply(e.getValue()));
            }
            return result;
        }

        @Override
        public boolean containsKey(String key) {
            return properties.containsKey(key);
        }

        @Override
        public Value get(String key) {
            return properties.getOrDefault(key, Values.NULL);
        }

        @Override
        public Iterable<String> keys() {
            return properties.keySet();
        }

        @Override
        public int size() {
            return properties.size();
        }

        @Override
        public Iterable<Value> values() {
            return properties.values();
        }

        @Override
        public <T> Iterable<T> values(Function<Value, T> mapFunction) {
            List<T> result = new ArrayList<>(properties.size());
            for (Value v : properties.values()) {
                result.add(mapFunction.apply(v));
            }
            return result;
        }

        @Override
        public boolean hasLabel(String label) {
            return labels.contains(label);
        }

        @Override
        public Iterable<String> labels() {
            return labels;
        }
    }

    private record JsonRelationshipEntity(long id, long startId, long endId, String type,
                                          Map<String, Value> properties) implements Relationship {

            private JsonRelationshipEntity(long id,
                                           long startId,
                                           long endId,
                                           String type,
                                           Map<String, Value> properties) {
                this.id = id;
                this.startId = startId;
                this.endId = endId;
                this.type = type;
                this.properties = Map.copyOf(properties);
            }

            @Override
            public String elementId() {
                return id + "";
            }

            @Override
            public Map<String, Object> asMap() {
                Map<String, Object> result = new LinkedHashMap<>();
                for (Map.Entry<String, Value> e : properties.entrySet()) {
                    result.put(e.getKey(), e.getValue().asObject());
                }
                return result;
            }

            @Override
            public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
                Map<String, T> result = new LinkedHashMap<>();
                for (Map.Entry<String, Value> e : properties.entrySet()) {
                    result.put(e.getKey(), mapFunction.apply(e.getValue()));
                }
                return result;
            }

            @Override
            public boolean containsKey(String key) {
                return properties.containsKey(key);
            }

            @Override
            public Value get(String key) {
                return properties.getOrDefault(key, Values.NULL);
            }

            @Override
            public Iterable<String> keys() {
                return properties.keySet();
            }

            @Override
            public int size() {
                return properties.size();
            }

            @Override
            public Iterable<Value> values() {
                return properties.values();
            }

            @Override
            public <T> Iterable<T> values(Function<Value, T> mapFunction) {
                List<T> result = new ArrayList<>(properties.size());
                for (Value v : properties.values()) {
                    result.add(mapFunction.apply(v));
                }
                return result;
            }

            @Override
            public boolean hasType(String relationshipType) {
                return type.equals(relationshipType);
            }

            @Override
            public long startNodeId() {
                return startId;
            }

            @Override
            public String startNodeElementId() {
                return startId + "";
            }

            @Override
            public long endNodeId() {
                return endId;
            }

            @Override
            public String endNodeElementId() {
                return endId + "";
            }
        }
}
