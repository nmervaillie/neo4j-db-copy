package org.neo4j.dbcopy.parquet;

import org.junit.jupiter.api.Test;
import org.neo4j.dbcopy.schema.PropertyType;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.DATE_TIME;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.INTEGER;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.STRING;

class ParquetFooterTest {

    private static final Path FILE = Path.of("/tmp/export/nodes/Person.parquet");

    @Test
    void should_round_trip_a_node_footer() {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("age", INTEGER);
        types.put("born", DATE_TIME);
        types.put("tags", new PropertyType.ListOf(STRING));
        ParquetFooter footer = ParquetFooter.forNode(new TreeSet<>(List.of("Employee", "Person")), types);

        Map<String, String> metadata = footer.toKeyValueMetadata();

        assertThat(metadata).containsEntry("neo4j.dbcopy.version", "1");
        assertThat(metadata).containsEntry("neo4j.recordType", "node");
        assertThat(ParquetFooter.fromKeyValueMetadata(metadata, FILE)).isEqualTo(footer);
    }

    @Test
    void should_round_trip_a_relationship_footer() {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("since", INTEGER);
        ParquetFooter footer = ParquetFooter.forRelationship("KNOWS", types);

        Map<String, String> metadata = footer.toKeyValueMetadata();

        assertThat(metadata).containsEntry("neo4j.recordType", "relationship");
        assertThat(metadata).containsEntry("neo4j.relationshipType", "KNOWS");
        assertThat(ParquetFooter.fromKeyValueMetadata(metadata, FILE)).isEqualTo(footer);
    }

    @Test
    void should_render_type_tokens_rather_than_enum_internals() {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("tags", new PropertyType.ListOf(STRING));

        Map<String, String> metadata = ParquetFooter.forNode(new TreeSet<>(), types).toKeyValueMetadata();

        assertThat(metadata.get("neo4j.types")).contains("LIST<STRING>");
    }

    @Test
    void should_reject_metadata_without_a_version() {
        assertThatThrownBy(() -> ParquetFooter.fromKeyValueMetadata(Map.of(), FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a neo4j-db-copy parquet export");
    }

    @Test
    void should_reject_an_unsupported_version() {
        Map<String, String> metadata = Map.of(
                "neo4j.dbcopy.version", "99",
                "neo4j.recordType", "node",
                "neo4j.labels", "[]",
                "neo4j.types", "{}");

        assertThatThrownBy(() -> ParquetFooter.fromKeyValueMetadata(metadata, FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version 99");
    }

    @Test
    void should_reject_a_relationship_footer_missing_a_relationship_type() {
        Map<String, String> metadata = Map.of(
                "neo4j.dbcopy.version", "1",
                "neo4j.recordType", "relationship",
                "neo4j.types", "{}");

        assertThatThrownBy(() -> ParquetFooter.fromKeyValueMetadata(metadata, FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incomplete neo4j-db-copy footer")
                .hasMessageContaining(FILE.toString());
    }

    @Test
    void should_reject_an_unknown_record_type() {
        Map<String, String> metadata = Map.of(
                "neo4j.dbcopy.version", "1",
                "neo4j.recordType", "banana",
                "neo4j.types", "{}");

        assertThatThrownBy(() -> ParquetFooter.fromKeyValueMetadata(metadata, FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("banana");
    }
}
