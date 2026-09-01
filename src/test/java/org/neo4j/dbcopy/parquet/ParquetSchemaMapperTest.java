package org.neo4j.dbcopy.parquet;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import org.junit.jupiter.api.Test;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.*;

class ParquetSchemaMapperTest {

    @Test
    void should_map_reserved_node_columns() {
        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(new TreeMap<>()));

        assertThat(paths(schema)).startsWith("__dbcopy_id", "__dbcopy_labels.list.element");
        assertThat(column(schema, "__dbcopy_id").type()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "__dbcopy_id").repetitionType()).isEqualTo(RepetitionType.REQUIRED);
    }

    @Test
    void should_map_reserved_relationship_columns() {
        FileSchema schema = ParquetSchemaMapper.relationshipFileSchema(
                new RelationshipTypeSchema("KNOWS", new TreeMap<>()));

        assertThat(paths(schema)).startsWith(
                "__dbcopy_id", "__dbcopy_source_id", "__dbcopy_target_id", "__dbcopy_type");
        assertThat(column(schema, "__dbcopy_source_id").type()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "__dbcopy_target_id").repetitionType())
                .isEqualTo(RepetitionType.REQUIRED);
    }

    @Test
    void should_map_scalar_properties_as_optional_columns() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("active", BOOLEAN);
        properties.put("age", INTEGER);
        properties.put("born", DATE);
        properties.put("name", STRING);
        properties.put("payload", BYTE_ARRAY);
        properties.put("ratio", FLOAT);
        properties.put("wakeup", LOCAL_TIME);

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(column(schema, "active").type()).isEqualTo(PhysicalType.BOOLEAN);
        assertThat(column(schema, "age").type()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "ratio").type()).isEqualTo(PhysicalType.DOUBLE);
        assertThat(column(schema, "name").logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(column(schema, "payload").type()).isEqualTo(PhysicalType.BYTE_ARRAY);
        assertThat(column(schema, "payload").logicalType()).isNull();
        assertThat(column(schema, "born").type()).isEqualTo(PhysicalType.INT32);
        assertThat(column(schema, "born").logicalType()).isEqualTo(new LogicalType.DateType());
        assertThat(column(schema, "wakeup").logicalType())
                .isEqualTo(new LogicalType.TimeType(false, LogicalType.TimeUnit.NANOS));
        assertThat(column(schema, "age").repetitionType()).isEqualTo(RepetitionType.OPTIONAL);
    }

    @Test
    void should_map_local_date_time_as_a_struct() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("seen", LOCAL_DATE_TIME);

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).contains("seen.epochSecond", "seen.nano");
        assertThat(column(schema, "seen.epochSecond").type()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "seen.nano").type()).isEqualTo(PhysicalType.INT32);
    }

    @Test
    void should_map_date_time_as_a_struct_with_a_nullable_zone_id() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("created", DATE_TIME);

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).contains(
                "created.epochSecond", "created.nano", "created.zoneId", "created.offsetSeconds");
        assertThat(column(schema, "created.zoneId").repetitionType()).isEqualTo(RepetitionType.OPTIONAL);
        assertThat(column(schema, "created.offsetSeconds").repetitionType())
                .isEqualTo(RepetitionType.REQUIRED);
    }

    @Test
    void should_map_offset_time_duration_and_point_as_structs() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("alarm", OFFSET_TIME);
        properties.put("elapsed", DURATION);
        properties.put("location", POINT);

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).contains("alarm.nanoOfDay", "alarm.offsetSeconds");
        assertThat(paths(schema)).contains(
                "elapsed.months", "elapsed.days", "elapsed.seconds", "elapsed.nanos");
        assertThat(paths(schema)).contains("location.srid", "location.x", "location.y", "location.z");
        assertThat(column(schema, "location.z").repetitionType()).isEqualTo(RepetitionType.OPTIONAL);
        assertThat(column(schema, "location.y").repetitionType()).isEqualTo(RepetitionType.REQUIRED);
    }

    @Test
    void should_map_lists() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("scores", new PropertyType.ListOf(INTEGER));
        properties.put("tags", new PropertyType.ListOf(STRING));

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).anyMatch(path -> path.startsWith("tags"));
        assertThat(paths(schema)).anyMatch(path -> path.startsWith("scores"));
        assertThat(column(schema, "scores.list.element").type()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "scores.list.element").logicalType()).isNull();
        assertThat(column(schema, "scores.list.element").repetitionType())
                .isEqualTo(RepetitionType.REQUIRED);
        assertThat(column(schema, "tags.list.element").type()).isEqualTo(PhysicalType.BYTE_ARRAY);
        assertThat(column(schema, "tags.list.element").logicalType())
                .isEqualTo(new LogicalType.StringType());
    }

    @Test
    void should_map_an_unknown_list_element_as_a_string() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("empties", new PropertyType.ListOf(UNKNOWN));

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).anyMatch(path -> path.startsWith("empties"));
        assertThat(column(schema, "empties.list.element").type()).isEqualTo(PhysicalType.BYTE_ARRAY);
        assertThat(column(schema, "empties.list.element").logicalType())
                .isEqualTo(new LogicalType.StringType());
    }

    private static NodeTypeSchema nodeType(SortedMap<String, PropertyType> properties) {
        return new NodeTypeSchema(new TreeSet<>(List.of("Person")), properties);
    }

    private static List<String> paths(FileSchema schema) {
        return schema.getColumns().stream().map(c -> c.fieldPath().toString()).toList();
    }

    private static ColumnSchema column(FileSchema schema, String path) {
        return schema.getColumns().stream()
                .filter(c -> c.fieldPath().toString().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column at path '" + path + "' in " + paths(schema)));
    }
}
