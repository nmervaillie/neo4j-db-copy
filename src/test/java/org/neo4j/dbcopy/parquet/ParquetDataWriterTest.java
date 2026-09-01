package org.neo4j.dbcopy.parquet;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.INTEGER;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.STRING;

class ParquetDataWriterTest {

    @Test
    void should_write_one_file_per_label_set(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada")));
        Node employee = new NodeRecord(2L, List.of("Person", "Employee"),
                Map.of("name", Values.value("Bob"), "empNo", Values.value(7L)));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.just(person, employee), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.empty(), CopyOptions.DEFAULT).blockLast();
        }

        assertThat(export.resolve("nodes/Person.parquet")).exists();
        assertThat(export.resolve("nodes/Employee__Person.parquet")).exists();
        assertThat(rowCount(export.resolve("nodes/Person.parquet"))).isEqualTo(1L);
        assertThat(rowCount(export.resolve("nodes/Employee__Person.parquet"))).isEqualTo(1L);
    }

    @Test
    void should_write_identity_columns_and_properties(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada")));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.just(person), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.empty(), CopyOptions.DEFAULT).blockLast();
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(export.resolve("nodes/Person.parquet")));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getLong("__dbcopy_id")).isEqualTo(1L);
            assertThat(rows.getList("__dbcopy_labels").strings()).containsExactly("Person");
            assertThat(rows.getString("name")).isEqualTo("Ada");
        }
    }

    @Test
    void should_write_relationships(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Relationship knows = new RelationshipRecord(7L, 1L, 2L, "KNOWS",
                Map.of("since", Values.value(2020L)));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.empty(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.just(knows), CopyOptions.DEFAULT).blockLast();
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(export.resolve("relationships/KNOWS.parquet")));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getLong("__dbcopy_id")).isEqualTo(7L);
            assertThat(rows.getLong("__dbcopy_source_id")).isEqualTo(1L);
            assertThat(rows.getLong("__dbcopy_target_id")).isEqualTo(2L);
            assertThat(rows.getString("__dbcopy_type")).isEqualTo("KNOWS");
            assertThat(rows.getLong("since")).isEqualTo(2020L);
        }
    }

    @Test
    void should_stamp_the_footer_metadata(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada")));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.just(person), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.empty(), CopyOptions.DEFAULT).blockLast();
        }

        Path file = export.resolve("nodes/Person.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            ParquetFooter footer = ParquetFooter.fromKeyValueMetadata(
                    reader.getFileMetaData().keyValueMetadata(), file);
            assertThat(footer.recordType()).isEqualTo("node");
            assertThat(footer.labels()).containsExactly("Person");
            assertThat(footer.types()).containsEntry("name", STRING);
        }
    }

    @Test
    void should_not_create_a_file_for_a_label_set_with_no_rows(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.empty(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(Flux.empty(), CopyOptions.DEFAULT).blockLast();
        }

        assertThat(Files.list(export.resolve("nodes")).toList()).isEmpty();
    }

    @Test
    void should_exclude_properties_listed_in_the_copy_options(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada")));
        CopyOptions options = new CopyOptions.Builder()
                .excludeNodeProperties(Set.of("name"))
                .build();

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.just(person), options).blockLast();
            writer.writeRelationships(Flux.empty(), options).blockLast();
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(export.resolve("nodes/Person.parquet")));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("name")).isTrue();
        }
    }

    @Test
    void should_fail_with_entity_context_on_a_type_mismatch(@TempDir Path dir) {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(4711L, List.of("Person"), Map.of("name", Values.value(1L)));

        assertThatThrownBy(() -> {
            try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
                writer.writeNodes(Flux.just(person), CopyOptions.DEFAULT).blockLast();
            }
        })
                .hasMessageContaining("node 4711")
                .hasMessageContaining("(:Person)")
                .hasMessageContaining("name")
                .hasMessageContaining("STRING")
                .hasMessageContaining("INTEGER");
    }

    @Test
    void should_fail_when_a_node_has_a_label_set_the_schema_does_not_know(@TempDir Path dir) {
        Path export = dir.resolve("export");
        Node ghost = new NodeRecord(9L, List.of("Ghost"), Map.of());

        assertThatThrownBy(() -> {
            try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
                writer.writeNodes(Flux.just(ghost), CopyOptions.DEFAULT).blockLast();
            }
        })
                .hasMessageContaining("Ghost")
                .hasMessageContaining("was not found in the scanned schema");
    }

    @Test
    void should_fail_when_a_node_has_a_property_the_schema_scan_did_not_see(@TempDir Path dir) {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"),
                Map.of("name", Values.value("Ada"), "legacyId", Values.value(42L)));

        assertThatThrownBy(() -> {
            try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
                writer.writeNodes(Flux.just(person), CopyOptions.DEFAULT).blockLast();
            }
        })
                .hasMessageContaining("node 1")
                .hasMessageContaining("(:Person)")
                .hasMessageContaining("legacyId")
                .hasMessageContaining("1000");
    }

    @Test
    void should_fail_when_a_relationship_has_a_property_the_schema_scan_did_not_see(@TempDir Path dir) {
        Path export = dir.resolve("export");
        Relationship knows = new RelationshipRecord(7L, 1L, 2L, "KNOWS",
                Map.of("since", Values.value(2020L), "note", Values.value("hi")));

        assertThatThrownBy(() -> {
            try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
                writer.writeRelationships(Flux.just(knows), CopyOptions.DEFAULT).blockLast();
            }
        })
                .hasMessageContaining("relationship 7")
                .hasMessageContaining("KNOWS")
                .hasMessageContaining("note")
                .hasMessageContaining("1000");
    }

    @Test
    void should_not_fail_when_an_unscanned_property_is_excluded(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Node person = new NodeRecord(1L, List.of("Person"),
                Map.of("name", Values.value("Ada"), "legacyId", Values.value(42L)));
        CopyOptions options = new CopyOptions.Builder()
                .excludeNodeProperties(Set.of("legacyId"))
                .build();

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
            writer.writeNodes(Flux.just(person), options).blockLast();
            writer.writeRelationships(Flux.empty(), options).blockLast();
        }

        assertThat(rowCount(export.resolve("nodes/Person.parquet"))).isEqualTo(1L);
    }

    @Test
    void should_reject_a_schema_with_a_reserved_property_name(@TempDir Path dir) {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("__dbcopy_id", INTEGER);
        GraphSchema bad = new GraphSchema(
                List.of(new NodeTypeSchema(new TreeSet<>(List.of("Person")), properties)), List.of());

        assertThatThrownBy(() -> new ParquetDataWriter(dir.resolve("export"), bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__dbcopy_id");
    }

    private static GraphSchema schema() {
        SortedMap<String, PropertyType> personProperties = new TreeMap<>();
        personProperties.put("name", STRING);

        SortedMap<String, PropertyType> employeeProperties = new TreeMap<>();
        employeeProperties.put("empNo", INTEGER);
        employeeProperties.put("name", STRING);

        SortedMap<String, PropertyType> knowsProperties = new TreeMap<>();
        knowsProperties.put("since", INTEGER);

        return new GraphSchema(
                List.of(new NodeTypeSchema(new TreeSet<>(List.of("Person")), personProperties),
                        new NodeTypeSchema(new TreeSet<>(List.of("Person", "Employee")), employeeProperties)),
                List.of(new RelationshipTypeSchema("KNOWS", knowsProperties)));
    }

    private static long rowCount(Path file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileMetaData().numRows();
        }
    }
}
