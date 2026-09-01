package org.neo4j.dbcopy.parquet;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;

import java.util.Map;

/**
 * Turns an inferred {@link org.neo4j.dbcopy.schema.GraphSchema} entry into a parquet file schema.
 * Types whose native parquet form would lose range, precision or a component are written as
 * structs; see the design doc for why.
 */
public final class ParquetSchemaMapper {

    private static final LogicalType.TimeType NANOS_OF_DAY =
            new LogicalType.TimeType(false, LogicalType.TimeUnit.NANOS);

    private ParquetSchemaMapper() {
    }

    public static FileSchema nodeFileSchema(NodeTypeSchema nodeType) {
        FileSchema.Builder builder = FileSchema.builder("node")
                .addColumn(ParquetLayout.ID, PhysicalType.INT64, RepetitionType.REQUIRED)
                .list(ParquetLayout.LABELS, RepetitionType.REQUIRED,
                        e -> e.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                new LogicalType.StringType()));
        addProperties(builder, nodeType.properties());
        return builder.build();
    }

    public static FileSchema relationshipFileSchema(RelationshipTypeSchema relType) {
        FileSchema.Builder builder = FileSchema.builder("relationship")
                .addColumn(ParquetLayout.ID, PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn(ParquetLayout.SOURCE_ID, PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn(ParquetLayout.TARGET_ID, PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn(ParquetLayout.TYPE, PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        new LogicalType.StringType());
        addProperties(builder, relType.properties());
        return builder.build();
    }

    private static void addProperties(FileSchema.Builder builder,
                                      Map<String, PropertyType> properties) {
        properties.forEach((name, type) -> addProperty(builder, name, type));
    }

    private static void addProperty(FileSchema.Builder builder, String name, PropertyType type) {
        if (type instanceof PropertyType.ListOf list) {
            builder.list(name, RepetitionType.OPTIONAL,
                    e -> addListElement(e, toScalar(list.element())));
            return;
        }
        addScalar(builder, name, toScalar(type));
    }

    private static void addScalar(FileSchema.Builder builder, String name, PropertyType.Scalar scalar) {
        RepetitionType optional = RepetitionType.OPTIONAL;
        switch (scalar) {
            case BOOLEAN -> builder.addColumn(name, PhysicalType.BOOLEAN, optional);
            case INTEGER -> builder.addColumn(name, PhysicalType.INT64, optional);
            case FLOAT -> builder.addColumn(name, PhysicalType.DOUBLE, optional);
            case STRING -> builder.addColumn(name, PhysicalType.BYTE_ARRAY, optional,
                    new LogicalType.StringType());
            case BYTE_ARRAY -> builder.addColumn(name, PhysicalType.BYTE_ARRAY, optional);
            case DATE -> builder.addColumn(name, PhysicalType.INT32, optional,
                    new LogicalType.DateType());
            case LOCAL_TIME -> builder.addColumn(name, PhysicalType.INT64, optional, NANOS_OF_DAY);
            case OFFSET_TIME -> builder.struct(name, optional, s -> s
                    .addColumn("nanoOfDay", PhysicalType.INT64, RepetitionType.REQUIRED, NANOS_OF_DAY)
                    .addColumn("offsetSeconds", PhysicalType.INT32, RepetitionType.REQUIRED));
            case LOCAL_DATE_TIME -> builder.struct(name, optional, s -> s
                    .addColumn("epochSecond", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nano", PhysicalType.INT32, RepetitionType.REQUIRED));
            case DATE_TIME -> builder.struct(name, optional, s -> s
                    .addColumn("epochSecond", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nano", PhysicalType.INT32, RepetitionType.REQUIRED)
                    .addColumn("zoneId", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                            new LogicalType.StringType())
                    .addColumn("offsetSeconds", PhysicalType.INT32, RepetitionType.REQUIRED));
            case DURATION -> builder.struct(name, optional, s -> s
                    .addColumn("months", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("days", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("seconds", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nanos", PhysicalType.INT32, RepetitionType.REQUIRED));
            case POINT -> builder.struct(name, optional, s -> s
                    .addColumn("srid", PhysicalType.INT32, RepetitionType.REQUIRED)
                    .addColumn("x", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                    .addColumn("y", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                    .addColumn("z", PhysicalType.DOUBLE, RepetitionType.OPTIONAL));
            case UNKNOWN -> throw new IllegalStateException("UNKNOWN must be resolved before mapping.");
        }
    }

    private static void addListElement(FileSchema.ElementBuilder element, PropertyType.Scalar scalar) {
        RepetitionType required = RepetitionType.REQUIRED;
        switch (scalar) {
            case BOOLEAN -> element.primitive(PhysicalType.BOOLEAN, required);
            case INTEGER -> element.primitive(PhysicalType.INT64, required);
            case FLOAT -> element.primitive(PhysicalType.DOUBLE, required);
            case STRING -> element.primitive(PhysicalType.BYTE_ARRAY, required,
                    new LogicalType.StringType());
            case BYTE_ARRAY -> element.primitive(PhysicalType.BYTE_ARRAY, required);
            case DATE -> element.primitive(PhysicalType.INT32, required, new LogicalType.DateType());
            case LOCAL_TIME -> element.primitive(PhysicalType.INT64, required, NANOS_OF_DAY);
            case OFFSET_TIME -> element.struct(required, s -> s
                    .addColumn("nanoOfDay", PhysicalType.INT64, RepetitionType.REQUIRED, NANOS_OF_DAY)
                    .addColumn("offsetSeconds", PhysicalType.INT32, RepetitionType.REQUIRED));
            case LOCAL_DATE_TIME -> element.struct(required, s -> s
                    .addColumn("epochSecond", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nano", PhysicalType.INT32, RepetitionType.REQUIRED));
            case DATE_TIME -> element.struct(required, s -> s
                    .addColumn("epochSecond", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nano", PhysicalType.INT32, RepetitionType.REQUIRED)
                    .addColumn("zoneId", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                            new LogicalType.StringType())
                    .addColumn("offsetSeconds", PhysicalType.INT32, RepetitionType.REQUIRED));
            case DURATION -> element.struct(required, s -> s
                    .addColumn("months", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("days", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("seconds", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("nanos", PhysicalType.INT32, RepetitionType.REQUIRED));
            case POINT -> element.struct(required, s -> s
                    .addColumn("srid", PhysicalType.INT32, RepetitionType.REQUIRED)
                    .addColumn("x", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                    .addColumn("y", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                    .addColumn("z", PhysicalType.DOUBLE, RepetitionType.OPTIONAL));
            case UNKNOWN -> throw new IllegalStateException("UNKNOWN must be resolved before mapping.");
        }
    }

    /**
     * Neo4j property lists are never nested and never contain maps, so a list element is always
     * scalar. UNKNOWN, the all-empty-list placeholder, becomes STRING.
     */
    private static PropertyType.Scalar toScalar(PropertyType type) {
        if (!(type instanceof PropertyType.Scalar scalar)) {
            throw new IllegalArgumentException(
                    "Nested lists are not supported as Neo4j properties: " + type);
        }
        return scalar == PropertyType.Scalar.UNKNOWN ? PropertyType.Scalar.STRING : scalar;
    }
}
