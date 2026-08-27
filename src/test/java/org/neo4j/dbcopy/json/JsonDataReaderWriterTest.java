package org.neo4j.dbcopy.json;

import org.junit.jupiter.api.Test;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonDataReaderWriterTest {

    @Test
    void should_round_trip_nodes_and_relationships() throws Exception {
        Path jsonDir = Files.createTempDirectory("dbcopy-roundtrip");
        Path jsonFile = jsonDir.resolve("roundtrip.jsonl");
        Node node = new TestNode(1L, List.of("Person", "Source"), Map.of(
                "name", Values.value("Alice"),
                "active", Values.value(true),
                "score", Values.value(42L),
                "ratio", Values.value(2.5d),
                "created", Values.value(LocalDate.of(2024, 1, 15)),
                "updated", Values.value(OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC)),
                "payload", Values.value(new byte[]{1, 2, 3}),
                "tags", Values.value(List.of("a", "b"))
        ));
        Relationship relationship = new TestRelationship(7L, 1L, 1L, "KNOWS", Map.of(
                "since", Values.value(2020L),
                "close", Values.value(false)
        ));

        try (JsonDataWriter writer = new JsonDataWriter(jsonFile)) {
            writer.writeNodes(Flux.just(node), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.just(relationship), CopyOptions.DEFAULT).blockLast();
        }

        List<Node> nodes;
        List<Relationship> relationships;
        try (JsonDataReader reader = new JsonDataReader(jsonFile)) {
            nodes = reader.readNodes().collectList().block();
            relationships = reader.readRelationships().collectList().block();
        }

        assertThat(nodes).hasSize(1);
        assertThat(relationships).hasSize(1);
        assertThat(nodes.get(0).labels()).containsExactlyInAnyOrder("Person", "Source");
        assertThat(nodes.get(0).asMap()).containsEntry("name", "Alice")
                .containsEntry("active", true)
                .containsEntry("score", 42L)
                .containsEntry("ratio", 2.5d)
                .containsEntry("created", LocalDate.of(2024, 1, 15))
                .containsEntry("tags", List.of("a", "b"));
        assertThat((byte[]) nodes.get(0).asMap().get("payload")).containsExactly(1, 2, 3);
        assertThat(nodes.get(0).asMap().get("updated"))
                .isEqualTo(ZonedDateTime.parse("2024-01-15T10:30Z"));
        assertThat(relationships.get(0).type()).isEqualTo("KNOWS");
        assertThat(relationships.get(0).asMap()).containsEntry("since", 2020L)
                .containsEntry("close", false);
    }

    @Test
    void should_write_one_compact_json_record_per_line() throws Exception {
        Path jsonDir = Files.createTempDirectory("dbcopy-jsonl-layout");
        Path jsonFile = jsonDir.resolve("layout.jsonl");
        Node node = new TestNode(1L, List.of("Person"), Map.of("name", Values.value("Alice")));
        Relationship relationship = new TestRelationship(7L, 1L, 1L, "KNOWS", Map.of("since", Values.value(2020L)));

        try (JsonDataWriter writer = new JsonDataWriter(jsonFile)) {
            writer.writeNodes(Flux.just(node), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.just(relationship), CopyOptions.DEFAULT).blockLast();
        }

        List<String> lines;
        try (Stream<String> stream = Files.lines(jsonFile)) {
            lines = stream.toList();
        }

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).doesNotContain("\n").startsWith("{").endsWith("}");
        assertThat(lines.get(1)).doesNotContain("\n").startsWith("{").endsWith("}");
    }

    @Test
    void should_round_trip_gzip_compressed_jsonl() throws Exception {
        Path jsonDir = Files.createTempDirectory("dbcopy-roundtrip-gzip");
        Path jsonFile = jsonDir.resolve("roundtrip.jsonl.gz");
        Node node = new TestNode(1L, List.of("Person"), Map.of("name", Values.value("Alice")));
        Relationship relationship = new TestRelationship(7L, 1L, 1L, "KNOWS", Map.of("since", Values.value(2020L)));

        try (JsonDataWriter writer = new JsonDataWriter(jsonFile)) {
            writer.writeNodes(Flux.just(node), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.just(relationship), CopyOptions.DEFAULT).blockLast();
        }

        List<Node> nodes;
        List<Relationship> relationships;
        try (JsonDataReader reader = new JsonDataReader(jsonFile)) {
            nodes = reader.readNodes().collectList().block();
            relationships = reader.readRelationships().collectList().block();
        }

        assertThat(nodes).hasSize(1);
        assertThat(nodes.get(0).asMap()).containsEntry("name", "Alice");
        assertThat(relationships).hasSize(1);
        assertThat(relationships.get(0).type()).isEqualTo("KNOWS");
    }

    @Test
    void should_fail_on_malformed_json_line() throws Exception {
        Path jsonFile = Files.createTempFile("dbcopy-malformed", ".jsonl");
        Files.writeString(jsonFile, "{\"recordType\":\"node\"\n");

        JsonDataReader reader = new JsonDataReader(jsonFile);

        assertThatThrownBy(() -> reader.readNodes().collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse JSONL line 1");
    }

    @Test
    void should_fail_on_missing_required_fields() throws Exception {
        Path jsonFile = Files.createTempFile("dbcopy-missing-fields", ".jsonl");
        Files.writeString(jsonFile, "{\"recordType\":\"node\",\"id\":1}\n");

        JsonDataReader reader = new JsonDataReader(jsonFile);

        assertThatThrownBy(() -> reader.readNodes().collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("labels");
    }

    @Test
    void should_fail_on_invalid_typed_value_payload() throws Exception {
        Path jsonFile = Files.createTempFile("dbcopy-invalid-type", ".jsonl");
        Files.writeString(jsonFile, """
                {"recordType":"node","id":1,"labels":["A"],"properties":{"payload":{"type":"ByteArray","value":"%%%bad"}}}
                """);

        JsonDataReader reader = new JsonDataReader(jsonFile);

        assertThatThrownBy(() -> reader.readNodes().collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid base64 byte array value");
    }

    @Test
    void should_fail_on_unsupported_export_value_type() throws Exception {
        Path jsonDir = Files.createTempDirectory("dbcopy-unsupported");
        Path jsonFile = jsonDir.resolve("unsupported.jsonl");
        Node node = new TestNode(1L, List.of("Unsupported"), Map.of(
                "payload", Values.value(Map.of("nested", "value"))
        ));

        try (JsonDataWriter writer = new JsonDataWriter(jsonFile)) {
            assertThatThrownBy(() -> writer.writeNodes(Flux.just(node), CopyOptions.DEFAULT).blockLast())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported property value type");
        }
    }

    @Test
    void should_fail_when_json_import_file_does_not_exist() {
        Path jsonFile = Path.of("does-not-exist-" + System.nanoTime() + ".jsonl");

        assertThatThrownBy(() -> new JsonDataReader(jsonFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON import file does not exist")
                .hasMessageContaining(jsonFile.toString());
    }

    @Test
    void should_fail_when_json_export_file_already_exists() throws Exception {
        Path jsonFile = Files.createTempFile("dbcopy-existing", ".jsonl");

        assertThatThrownBy(() -> new JsonDataWriter(jsonFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Refusing to overwrite existing JSON export file");
    }

    private static final class TestNode implements Node {
        private final long id;
        private final List<String> labels;
        private final Map<String, Value> values;

        private TestNode(long id, List<String> labels, Map<String, Value> values) {
            this.id = id;
            this.labels = List.copyOf(labels);
            this.values = Map.copyOf(values);
        }

        @Override
        public String elementId() {
            return String.valueOf(id);
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public Map<String, Object> asMap() {
            Map<String, Object> result = new LinkedHashMap<>();
            values.forEach((key, value) -> result.put(key, value.asObject()));
            return result;
        }

        @Override
        public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
            Map<String, T> result = new LinkedHashMap<>();
            values.forEach((key, value) -> result.put(key, mapFunction.apply(value)));
            return result;
        }

        @Override
        public boolean containsKey(String key) {
            return values.containsKey(key);
        }

        @Override
        public Value get(String key) {
            return values.getOrDefault(key, Values.NULL);
        }

        @Override
        public Iterable<String> keys() {
            return values.keySet();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Iterable<Value> values() {
            return values.values();
        }

        @Override
        public <T> Iterable<T> values(Function<Value, T> mapFunction) {
            List<T> mapped = new ArrayList<>(values.size());
            values.values().forEach(value -> mapped.add(mapFunction.apply(value)));
            return mapped;
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

    private static final class TestRelationship implements Relationship {
        private final long id;
        private final long startId;
        private final long endId;
        private final String type;
        private final Map<String, Value> values;

        private TestRelationship(long id, long startId, long endId, String type, Map<String, Value> values) {
            this.id = id;
            this.startId = startId;
            this.endId = endId;
            this.type = type;
            this.values = Map.copyOf(values);
        }

        @Override
        public String elementId() {
            return String.valueOf(id);
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public Map<String, Object> asMap() {
            Map<String, Object> result = new LinkedHashMap<>();
            values.forEach((key, value) -> result.put(key, value.asObject()));
            return result;
        }

        @Override
        public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
            Map<String, T> result = new LinkedHashMap<>();
            values.forEach((key, value) -> result.put(key, mapFunction.apply(value)));
            return result;
        }

        @Override
        public boolean containsKey(String key) {
            return values.containsKey(key);
        }

        @Override
        public Value get(String key) {
            return values.getOrDefault(key, Values.NULL);
        }

        @Override
        public Iterable<String> keys() {
            return values.keySet();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Iterable<Value> values() {
            return values.values();
        }

        @Override
        public <T> Iterable<T> values(Function<Value, T> mapFunction) {
            List<T> mapped = new ArrayList<>(values.size());
            values.values().forEach(value -> mapped.add(mapFunction.apply(value)));
            return mapped;
        }

        @Override
        public String type() {
            return type;
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
            return String.valueOf(startId);
        }

        @Override
        public long endNodeId() {
            return endId;
        }

        @Override
        public String endNodeElementId() {
            return String.valueOf(endId);
        }
    }
}
