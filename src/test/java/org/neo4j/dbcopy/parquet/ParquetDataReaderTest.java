package org.neo4j.dbcopy.parquet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.io.NodeRecord;
import org.neo4j.dbcopy.io.RelationshipRecord;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.*;

class ParquetDataReaderTest {

    @Test
    void should_round_trip_every_supported_type(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("active", BOOLEAN);
        types.put("age", INTEGER);
        types.put("alarm", OFFSET_TIME);
        types.put("born", DATE);
        types.put("createdOffset", DATE_TIME);
        types.put("createdZoned", DATE_TIME);
        types.put("elapsed", DURATION);
        types.put("empties", new PropertyType.ListOf(UNKNOWN));
        types.put("here", POINT);
        types.put("name", STRING);
        types.put("payload", BYTE_ARRAY);
        types.put("ratio", FLOAT);
        types.put("scores", new PropertyType.ListOf(INTEGER));
        types.put("seen", LOCAL_DATE_TIME);
        types.put("tags", new PropertyType.ListOf(STRING));
        types.put("there", POINT);
        types.put("wakeup", LOCAL_TIME);

        ZonedDateTime zoned = ZonedDateTime.parse("2024-01-15T10:30:00.000000042+01:00[Europe/Paris]");
        OffsetDateTime offset = OffsetDateTime.parse("2024-01-15T10:30:00+02:00");
        Map<String, Value> properties = Map.ofEntries(
                Map.entry("active", Values.value(true)),
                Map.entry("age", Values.value(36L)),
                Map.entry("alarm", Values.value(OffsetTime.of(LocalTime.of(7, 30), ZoneOffset.ofHours(2)))),
                Map.entry("born", Values.value(LocalDate.of(1815, 12, 10))),
                Map.entry("createdOffset", Values.value(offset)),
                Map.entry("createdZoned", Values.value(zoned)),
                Map.entry("elapsed", Values.isoDuration(1, 2, 3, 4)),
                Map.entry("empties", Values.value(List.of())),
                Map.entry("here", Values.point(4326, 2.35, 48.85)),
                Map.entry("name", Values.value("Ada")),
                Map.entry("payload", Values.value(new byte[]{1, 2, 3})),
                Map.entry("ratio", Values.value(2.5d)),
                Map.entry("scores", Values.value(List.of(1L, 2L, 3L))),
                Map.entry("seen", Values.value(LocalDateTime.of(2024, 1, 15, 10, 30, 0, 42))),
                Map.entry("tags", Values.value(List.of("a", "b"))),
                Map.entry("there", Values.point(4979, 1.0, 2.0, 3.0)),
                Map.entry("wakeup", Values.value(LocalTime.of(7, 30, 0, 42))));

        Path export = write(dir, types, new NodeRecord(1L, List.of("Person"), properties), null);

        List<Node> nodes;
        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            nodes = reader.readNodes().collectList().block();
        }

        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.id()).isEqualTo(1L);
        assertThat(node.labels()).containsExactly("Person");
        Map<String, Object> read = node.asMap();
        assertThat(read).containsEntry("active", true)
                .containsEntry("age", 36L)
                .containsEntry("ratio", 2.5d)
                .containsEntry("name", "Ada")
                .containsEntry("born", LocalDate.of(1815, 12, 10))
                .containsEntry("wakeup", LocalTime.of(7, 30, 0, 42))
                .containsEntry("alarm", OffsetTime.of(LocalTime.of(7, 30), ZoneOffset.ofHours(2)))
                .containsEntry("seen", LocalDateTime.of(2024, 1, 15, 10, 30, 0, 42))
                .containsEntry("createdZoned", zoned)
                // the neo4j-java-driver normalises any DATE_TIME property to ZonedDateTime on
                // asObject() (Values.value(OffsetDateTime) calls OffsetDateTime.toZonedDateTime()
                // internally), so the round-tripped value can never come back as an OffsetDateTime
                .containsEntry("createdOffset", offset.toZonedDateTime())
                .containsEntry("tags", List.of("a", "b"))
                .containsEntry("scores", List.of(1L, 2L, 3L))
                .containsEntry("empties", List.of());
        assertThat((byte[]) read.get("payload")).containsExactly(1, 2, 3);
        assertThat(node.get("elapsed").asIsoDuration().months()).isEqualTo(1L);
        assertThat(node.get("elapsed").asIsoDuration().nanoseconds()).isEqualTo(4);
        assertThat(node.get("here").asPoint().srid()).isEqualTo(4326);
        assertThat(node.get("here").asPoint().z()).isNaN();
        assertThat(node.get("there").asPoint().z()).isEqualTo(3.0);
    }

    @Test
    void should_omit_a_property_that_is_null_in_the_file(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("age", INTEGER);
        types.put("name", STRING);

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))), null);

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            Node node = reader.readNodes().blockFirst();
            assertThat(node.keys()).containsExactly("name");
            assertThat(node.containsKey("age")).isFalse();
        }
    }

    @Test
    void should_read_relationships(@TempDir Path dir) throws Exception {
        Path export = write(dir, new TreeMap<>(), null,
                new RelationshipRecord(7L, 1L, 2L, "KNOWS", Map.of("since", Values.value(2020L))));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            Relationship relationship = reader.readRelationships().blockFirst();
            assertThat(relationship.id()).isEqualTo(7L);
            assertThat(relationship.startNodeId()).isEqualTo(1L);
            assertThat(relationship.endNodeId()).isEqualTo(2L);
            assertThat(relationship.type()).isEqualTo("KNOWS");
            assertThat(relationship.asMap()).containsEntry("since", 2020L);
        }
    }

    @Test
    void should_count_rows_from_the_footers(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("name", STRING);

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))),
                new RelationshipRecord(7L, 1L, 1L, "KNOWS", Map.of()));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            assertThat(reader.getTotalNodeCount()).isEqualTo(1L);
            assertThat(reader.getTotalRelationshipCount()).isEqualTo(1L);
        }
    }

    @Test
    void should_rebuild_the_schema_from_the_footers(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("name", STRING);
        types.put("tags", new PropertyType.ListOf(STRING));

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))), null);

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            GraphSchema schema = reader.scanSchema(CopyOptions.DEFAULT);
            assertThat(schema.nodeTypes()).hasSize(1);
            assertThat(schema.nodeTypes().get(0).labels()).containsExactly("Person");
            assertThat(schema.nodeTypes().get(0).properties())
                    .containsEntry("name", STRING)
                    .containsEntry("tags", new PropertyType.ListOf(STRING));
        }
    }

    @Test
    void should_reject_a_directory_that_is_not_an_export(@TempDir Path dir) {
        assertThatThrownBy(() -> new ParquetDataReader(dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a neo4j-db-copy parquet export");
    }

    @Test
    void should_reject_a_parquet_file_without_a_dbcopy_footer(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Files.createDirectories(export.resolve("nodes"));
        Files.copy(foreignParquetFile(dir), export.resolve("nodes/foreign.parquet"));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            assertThatThrownBy(() -> reader.readNodes().collectList().block())
                    .hasMessageContaining("is not a neo4j-db-copy parquet export");
        }
    }

    /** A valid parquet file with no {@code neo4j.*} footer keys. */
    private static Path foreignParquetFile(Path dir) throws Exception {
        Path file = dir.resolve("foreign-source.parquet");
        dev.hardwood.schema.FileSchema schema = dev.hardwood.schema.FileSchema.builder("foreign")
                .addColumn("x", dev.hardwood.metadata.PhysicalType.INT64,
                        dev.hardwood.metadata.RepetitionType.REQUIRED)
                .build();
        try (dev.hardwood.writer.ParquetFileWriter writer = dev.hardwood.writer.ParquetFileWriter
                .create(dev.hardwood.OutputFile.of(file), schema)) {
            writer.rowWriter().writeRow(row -> row.setLong("x", 1L));
        }
        return file;
    }

    private static Path write(Path dir,
                              SortedMap<String, PropertyType> nodeProperties,
                              NodeRecord node,
                              RelationshipRecord relationship) throws Exception {
        Path export = dir.resolve("export-" + System.nanoTime());
        SortedMap<String, PropertyType> relProperties = new TreeMap<>();
        if (relationship != null) {
            relationship.properties().forEach((name, value) ->
                    relProperties.put(name, org.neo4j.dbcopy.schema.Neo4jTypeInference.inferType(value)));
        }
        GraphSchema schema = new GraphSchema(
                List.of(new NodeTypeSchema(new TreeSet<>(List.of("Person")), nodeProperties)),
                List.of(new RelationshipTypeSchema("KNOWS", relProperties)));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema)) {
            writer.writeNodes(node == null ? Flux.empty() : Flux.just(node), CopyOptions.DEFAULT)
                    .blockLast();
            writer.writeRelationships(
                    relationship == null ? Flux.empty() : Flux.just(relationship), CopyOptions.DEFAULT)
                    .blockLast();
        }
        return export;
    }
}
