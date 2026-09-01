package org.neo4j.dbcopy.parquet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetLayoutTest {

    @Test
    void should_name_a_node_file_after_its_sorted_labels() {
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("Person"))))
                .isEqualTo("Person.parquet");
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("Employee", "Person"))))
                .isEqualTo("Employee__Person.parquet");
    }

    @Test
    void should_name_the_unlabelled_node_file() {
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>())).isEqualTo("_no_labels.parquet");
    }

    @Test
    void should_escape_characters_that_are_illegal_in_file_names() {
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("a/b"))))
                .isEqualTo("a%2Fb.parquet");
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("a b"))))
                .isEqualTo("a%20b.parquet");
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("100%"))))
                .isEqualTo("100%25.parquet");
    }

    @Test
    void should_keep_underscores_readable() {
        assertThat(ParquetLayout.nodeFileName(new TreeSet<>(List.of("My_Label"))))
                .isEqualTo("My_Label.parquet");
    }

    @Test
    void should_name_a_relationship_file_after_its_type() {
        assertThat(ParquetLayout.relationshipFileName("ACTED_IN")).isEqualTo("ACTED_IN.parquet");
    }

    @Test
    void should_reject_properties_that_collide_with_reserved_columns() {
        assertThatThrownBy(() -> ParquetLayout.requireNoReservedProperties(
                "label set :Person", List.of("name", "__dbcopy_id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("label set :Person")
                .hasMessageContaining("__dbcopy_id");
    }

    @Test
    void should_accept_properties_that_do_not_collide() {
        ParquetLayout.requireNoReservedProperties("label set :Person", List.of("name", "age"));
    }

    @Test
    void should_reject_two_label_sets_that_map_to_the_same_file_name() {
        assertThatThrownBy(() -> ParquetLayout.requireDistinctFileNames(
                List.of("A__B.parquet", "A__B.parquet")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("A__B.parquet");
    }

    @Test
    void should_create_the_export_directory_layout(@TempDir Path dir) {
        Path target = dir.resolve("export");

        ParquetLayout.prepareExportDirectory(target);

        assertThat(target.resolve("nodes")).isDirectory();
        assertThat(target.resolve("relationships")).isDirectory();
    }

    @Test
    void should_refuse_a_non_empty_export_directory(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("stale.txt"), "x");

        assertThatThrownBy(() -> ParquetLayout.prepareExportDirectory(dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Refusing to write into non-empty parquet export directory");
    }

    @Test
    void should_reject_an_import_directory_that_is_not_an_export(@TempDir Path dir) {
        assertThatThrownBy(() -> ParquetLayout.requireImportDirectory(dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a neo4j-db-copy parquet export");
    }

    @Test
    void should_accept_an_import_directory_with_a_nodes_folder(@TempDir Path dir) throws Exception {
        Files.createDirectory(dir.resolve("nodes"));

        ParquetLayout.requireImportDirectory(dir);
    }

    @Test
    void should_reject_a_missing_import_directory(@TempDir Path dir) {
        assertThatThrownBy(() -> ParquetLayout.requireImportDirectory(dir.resolve("nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist");
    }
}
