package org.neo4j.dbcopy.parquet;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

class ParquetValuesWriteTest {

    @Test
    void should_write_scalars_and_read_them_back(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("active", BOOLEAN);
        types.put("age", INTEGER);
        types.put("born", DATE);
        types.put("name", STRING);
        types.put("payload", BYTE_ARRAY);
        types.put("ratio", FLOAT);
        types.put("wakeup", LOCAL_TIME);

        Map<String, Value> values = Map.of(
                "active", Values.value(true),
                "age", Values.value(42L),
                "born", Values.value(LocalDate.of(2024, 1, 15)),
                "name", Values.value("Ada"),
                "payload", Values.value(new byte[]{1, 2, 3}),
                "ratio", Values.value(2.5d),
                "wakeup", Values.value(LocalTime.of(7, 30)));

        try (RowReader rows = writeOneRow(dir, types, values)) {
            rows.next();
            assertThat(rows.getBoolean("active")).isTrue();
            assertThat(rows.getLong("age")).isEqualTo(42L);
            assertThat(rows.getDate("born")).isEqualTo(LocalDate.of(2024, 1, 15));
            assertThat(rows.getString("name")).isEqualTo("Ada");
            assertThat(rows.getBinary("payload")).containsExactly(1, 2, 3);
            assertThat(rows.getDouble("ratio")).isEqualTo(2.5d);
            assertThat(rows.getTime("wakeup")).isEqualTo(LocalTime.of(7, 30));
        }
    }

    @Test
    void should_write_a_zoned_date_time_keeping_its_zone_id(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("created", DATE_TIME);
        ZonedDateTime created = ZonedDateTime.parse("2024-01-15T10:30:00.123456789+01:00[Europe/Paris]");

        try (RowReader rows = writeOneRow(dir, types, Map.of("created", Values.value(created)))) {
            rows.next();
            PqStruct struct = rows.getStruct("created");
            assertThat(struct.getLong("epochSecond")).isEqualTo(created.toEpochSecond());
            assertThat(struct.getInt("nano")).isEqualTo(123456789);
            assertThat(struct.getString("zoneId")).isEqualTo("Europe/Paris");
            assertThat(struct.getInt("offsetSeconds")).isEqualTo(3600);
        }
    }

    @Test
    void should_write_an_offset_only_date_time_with_a_null_zone_id(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("created", DATE_TIME);

        try (RowReader rows = writeOneRow(dir, types,
                Map.of("created", Values.value(ZonedDateTime.parse("2024-01-15T10:30Z"))))) {
            rows.next();
            PqStruct struct = rows.getStruct("created");
            assertThat(struct.isNull("zoneId")).isTrue();
            assertThat(struct.getInt("offsetSeconds")).isZero();
        }
    }

    @Test
    void should_write_the_remaining_struct_types(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("alarm", OFFSET_TIME);
        types.put("elapsed", DURATION);
        types.put("here", POINT);
        types.put("seen", LOCAL_DATE_TIME);
        types.put("there", POINT);

        Map<String, Value> values = Map.of(
                "alarm", Values.value(OffsetTime.of(LocalTime.of(7, 30), ZoneOffset.ofHours(2))),
                "elapsed", Values.isoDuration(1, 2, 3, 4),
                "here", Values.point(4326, 2.35, 48.85),
                "seen", Values.value(LocalDateTime.of(2024, 1, 15, 10, 30, 0, 42)),
                "there", Values.point(4979, 1.0, 2.0, 3.0));

        try (RowReader rows = writeOneRow(dir, types, values)) {
            rows.next();
            assertThat(rows.getStruct("alarm").getInt("offsetSeconds")).isEqualTo(7200);
            assertThat(rows.getStruct("alarm").getLong("nanoOfDay"))
                    .isEqualTo(LocalTime.of(7, 30).toNanoOfDay());
            assertThat(rows.getStruct("elapsed").getLong("months")).isEqualTo(1L);
            assertThat(rows.getStruct("elapsed").getInt("nanos")).isEqualTo(4);
            assertThat(rows.getStruct("seen").getInt("nano")).isEqualTo(42);
            assertThat(rows.getStruct("here").getInt("srid")).isEqualTo(4326);
            assertThat(rows.getStruct("here").isNull("z")).isTrue();
            assertThat(rows.getStruct("there").getDouble("z")).isEqualTo(3.0);
        }
    }

    @Test
    void should_write_lists(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("empties", new PropertyType.ListOf(UNKNOWN));
        types.put("scores", new PropertyType.ListOf(INTEGER));
        types.put("tags", new PropertyType.ListOf(STRING));

        Map<String, Value> values = Map.of(
                "empties", Values.value(List.of()),
                "scores", Values.value(List.of(1L, 2L)),
                "tags", Values.value(List.of("a", "b")));

        try (RowReader rows = writeOneRow(dir, types, values)) {
            rows.next();
            assertThat(rows.getList("tags").strings()).containsExactly("a", "b");
            assertThat(rows.getList("scores").longs().size()).isEqualTo(2);
            assertThat(rows.getList("empties").isEmpty()).isTrue();
        }
    }

    @Test
    void should_write_null_for_an_absent_property(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("name", STRING);
        types.put("here", POINT);

        try (RowReader rows = writeOneRow(dir, types, Map.of())) {
            rows.next();
            assertThat(rows.isNull("name")).isTrue();
            assertThat(rows.isNull("here")).isTrue();
        }
    }

    @Test
    void should_reject_a_value_whose_type_does_not_match_the_schema(@TempDir Path dir) {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("age", INTEGER);

        assertThatThrownBy(() -> writeOneRow(dir, types, Map.of("age", Values.value("old"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("age")
                .hasMessageContaining("INTEGER")
                .hasMessageContaining("STRING");
    }

    @Test
    void should_reject_a_date_beyond_the_parquet_day_range(@TempDir Path dir) {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("born", DATE);

        assertThatThrownBy(() -> writeOneRow(dir, types,
                Map.of("born", Values.value(LocalDate.of(999_999_999, 1, 1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("born")
                .hasMessageContaining("out of range");
    }

    private static RowReader writeOneRow(Path dir,
                                         SortedMap<String, PropertyType> types,
                                         Map<String, Value> values) throws Exception {
        Path file = dir.resolve(values.hashCode() + ".parquet");
        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(
                new NodeTypeSchema(new TreeSet<>(List.of("Person")), types));

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.rowWriter().writeRow(row -> {
                row.setLong(ParquetLayout.ID, 1L);
                row.setList(ParquetLayout.LABELS, labels -> labels.addString("Person"));
                types.forEach((name, type) ->
                        ParquetValues.set(row, name, type, values.get(name)));
            });
        }
        return ParquetFileReader.open(InputFile.of(file)).rowReader();
    }
}
