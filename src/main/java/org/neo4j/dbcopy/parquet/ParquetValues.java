package org.neo4j.dbcopy.parquet;

import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.writer.ListBuilder;
import dev.hardwood.writer.StructBuilder;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Converts between Neo4j property values and parquet row fields. The struct layouts here must stay
 * in lockstep with {@link ParquetSchemaMapper}.
 */
final class ParquetValues {

    private ParquetValues() {
    }

    static void set(StructBuilder row, String field, PropertyType type, Value value) {
        if (value == null || value.isNull()) {
            row.setNull(field);
            return;
        }
        Object o = value.asObject();
        if (type instanceof PropertyType.ListOf list) {
            if (!(o instanceof List<?> elements)) {
                throw mismatch(field, type, value);
            }
            PropertyType.Scalar element = elementType(list);
            row.setList(field, builder -> addElements(builder, field, element, elements));
            return;
        }
        setScalar(row, field, (PropertyType.Scalar) type, o, value);
    }

    private static void setScalar(StructBuilder row, String field, PropertyType.Scalar type,
                                  Object o, Value value) {
        switch (type) {
            case BOOLEAN -> row.setBoolean(field, require(field, type, value, o, Boolean.class));
            case INTEGER -> row.setLong(field, require(field, type, value, o, Long.class));
            case FLOAT -> row.setDouble(field, require(field, type, value, o, Double.class));
            case STRING, UNKNOWN -> row.setString(field, require(field, type, value, o, String.class));
            case BYTE_ARRAY -> row.setBinary(field, require(field, type, value, o, byte[].class));
            case DATE -> row.setDate(field, requireDateInRange(field,
                    require(field, type, value, o, LocalDate.class)));
            case LOCAL_TIME -> row.setTime(field, require(field, type, value, o, LocalTime.class));
            case OFFSET_TIME -> row.setStruct(field,
                    offsetTime(require(field, type, value, o, OffsetTime.class)));
            case LOCAL_DATE_TIME -> row.setStruct(field,
                    localDateTime(require(field, type, value, o, LocalDateTime.class)));
            case DATE_TIME -> row.setStruct(field, dateTime(field, type, value, o));
            case DURATION -> row.setStruct(field,
                    duration(require(field, type, value, o, IsoDuration.class)));
            case POINT -> row.setStruct(field, point(require(field, type, value, o, Point.class)));
        }
    }

    private static void addElements(ListBuilder builder, String field, PropertyType.Scalar type,
                                    List<?> elements) {
        for (Object element : elements) {
            Value value = Values.value(element);
            switch (type) {
                case BOOLEAN -> builder.addBoolean(require(field, type, value, element, Boolean.class));
                case INTEGER -> builder.addLong(require(field, type, value, element, Long.class));
                case FLOAT -> builder.addDouble(require(field, type, value, element, Double.class));
                case STRING, UNKNOWN -> builder.addString(
                        require(field, type, value, element, String.class));
                case BYTE_ARRAY -> builder.addBinary(require(field, type, value, element, byte[].class));
                case DATE -> builder.addDate(requireDateInRange(field,
                        require(field, type, value, element, LocalDate.class)));
                case LOCAL_TIME -> builder.addTime(
                        require(field, type, value, element, LocalTime.class));
                case OFFSET_TIME -> builder.addStruct(
                        offsetTime(require(field, type, value, element, OffsetTime.class)));
                case LOCAL_DATE_TIME -> builder.addStruct(
                        localDateTime(require(field, type, value, element, LocalDateTime.class)));
                case DATE_TIME -> builder.addStruct(dateTime(field, type, value, element));
                case DURATION -> builder.addStruct(
                        duration(require(field, type, value, element, IsoDuration.class)));
                case POINT -> builder.addStruct(
                        point(require(field, type, value, element, Point.class)));
            }
        }
    }

    private static Consumer<StructBuilder> offsetTime(OffsetTime value) {
        return struct -> struct
                .setTime("nanoOfDay", value.toLocalTime())
                .setInt("offsetSeconds", value.getOffset().getTotalSeconds());
    }

    private static Consumer<StructBuilder> localDateTime(LocalDateTime value) {
        return struct -> struct
                .setLong("epochSecond", value.toEpochSecond(ZoneOffset.UTC))
                .setInt("nano", value.getNano());
    }

    private static Consumer<StructBuilder> dateTime(String field, PropertyType type, Value value,
                                                    Object o) {
        long epochSecond;
        int nano;
        String zoneId;
        int offsetSeconds;
        if (o instanceof ZonedDateTime zoned) {
            epochSecond = zoned.toEpochSecond();
            nano = zoned.getNano();
            zoneId = zoned.getZone() instanceof ZoneOffset ? null : zoned.getZone().getId();
            offsetSeconds = zoned.getOffset().getTotalSeconds();
        } else if (o instanceof OffsetDateTime offset) {
            epochSecond = offset.toEpochSecond();
            nano = offset.getNano();
            zoneId = null;
            offsetSeconds = offset.getOffset().getTotalSeconds();
        } else {
            throw mismatch(field, type, value);
        }
        return struct -> struct
                .setLong("epochSecond", epochSecond)
                .setInt("nano", nano)
                .setString("zoneId", zoneId)
                .setInt("offsetSeconds", offsetSeconds);
    }

    private static Consumer<StructBuilder> duration(IsoDuration value) {
        return struct -> struct
                .setLong("months", value.months())
                .setLong("days", value.days())
                .setLong("seconds", value.seconds())
                .setInt("nanos", value.nanoseconds());
    }

    private static Consumer<StructBuilder> point(Point value) {
        return struct -> {
            struct.setInt("srid", value.srid())
                    .setDouble("x", value.x())
                    .setDouble("y", value.y());
            if (Double.isNaN(value.z())) {
                struct.setNull("z");
            } else {
                struct.setDouble("z", value.z());
            }
        };
    }

    static PropertyType.Scalar elementType(PropertyType.ListOf list) {
        if (!(list.element() instanceof PropertyType.Scalar scalar)) {
            throw new IllegalArgumentException(
                    "Nested lists are not supported as Neo4j properties: " + list);
        }
        return scalar == PropertyType.Scalar.UNKNOWN ? PropertyType.Scalar.STRING : scalar;
    }

    private static LocalDate requireDateInRange(String field, LocalDate date) {
        long epochDay = date.toEpochDay();
        if (epochDay < Integer.MIN_VALUE || epochDay > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("property '" + field + "' holds the date " + date
                    + ", which is out of range for a parquet DATE column (days since epoch must fit"
                    + " in a 32-bit integer).");
        }
        return date;
    }

    @SuppressWarnings("unchecked")
    private static <T> T require(String field, PropertyType expected, Value value, Object o,
                                 Class<T> javaType) {
        if (!javaType.isInstance(o)) {
            throw mismatch(field, expected, value);
        }
        return (T) o;
    }

    private static IllegalArgumentException mismatch(String field, PropertyType expected, Value value) {
        String actual;
        try {
            actual = Neo4jTypeInference.toToken(Neo4jTypeInference.inferType(value));
        } catch (RuntimeException e) {
            actual = String.valueOf(value.asObject());
        }
        return new IllegalArgumentException("property '" + field + "' is of type " + actual
                + ", but the schema scan inferred " + Neo4jTypeInference.toToken(expected)
                + ". Property types must be consistent for parquet export.");
    }

    /**
     * @return the property value, or {@code null} when the field is null in the file. Neo4j has no
     * null property value, so callers omit the property entirely in that case.
     */
    static Value read(StructAccessor row, String field, PropertyType type) {
        if (row.isNull(field)) {
            return null;
        }
        if (type instanceof PropertyType.ListOf list) {
            return readList(row.getList(field), elementType(list));
        }
        return readScalar(row, field, (PropertyType.Scalar) type);
    }

    private static Value readScalar(StructAccessor row, String field, PropertyType.Scalar type) {
        return switch (type) {
            case BOOLEAN -> Values.value(row.getBoolean(field));
            case INTEGER -> Values.value(row.getLong(field));
            case FLOAT -> Values.value(row.getDouble(field));
            case STRING, UNKNOWN -> Values.value(row.getString(field));
            case BYTE_ARRAY -> Values.value(row.getBinary(field));
            case DATE -> Values.value(row.getDate(field));
            case LOCAL_TIME -> Values.value(row.getTime(field));
            case OFFSET_TIME -> Values.value(readOffsetTime(row.getStruct(field)));
            case LOCAL_DATE_TIME -> Values.value(readLocalDateTime(row.getStruct(field)));
            case DATE_TIME -> Values.value(readDateTime(row.getStruct(field)));
            case DURATION -> readDuration(row.getStruct(field));
            case POINT -> readPoint(row.getStruct(field));
        };
    }

    private static Value readList(PqList list, PropertyType.Scalar element) {
        List<Object> values = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            values.add(readElement(list, i, element));
        }
        return Values.value(values);
    }

    private static Object readElement(PqList list, int index, PropertyType.Scalar element) {
        return switch (element) {
            case OFFSET_TIME -> readOffsetTime((PqStruct) list.get(index));
            case LOCAL_DATE_TIME -> readLocalDateTime((PqStruct) list.get(index));
            case DATE_TIME -> readDateTime((PqStruct) list.get(index));
            case DURATION -> readDuration((PqStruct) list.get(index)).asObject();
            case POINT -> readPoint((PqStruct) list.get(index)).asObject();
            default -> list.get(index);
        };
    }

    private static OffsetTime readOffsetTime(StructAccessor struct) {
        return OffsetTime.of(struct.getTime("nanoOfDay"),
                ZoneOffset.ofTotalSeconds(struct.getInt("offsetSeconds")));
    }

    private static LocalDateTime readLocalDateTime(StructAccessor struct) {
        return LocalDateTime.ofEpochSecond(
                struct.getLong("epochSecond"), struct.getInt("nano"), ZoneOffset.UTC);
    }

    private static Object readDateTime(StructAccessor struct) {
        Instant instant = Instant.ofEpochSecond(struct.getLong("epochSecond"), struct.getInt("nano"));
        if (struct.isNull("zoneId")) {
            return OffsetDateTime.ofInstant(instant,
                    ZoneOffset.ofTotalSeconds(struct.getInt("offsetSeconds")));
        }
        return ZonedDateTime.ofInstant(instant, ZoneId.of(struct.getString("zoneId")));
    }

    private static Value readDuration(StructAccessor struct) {
        return Values.isoDuration(struct.getLong("months"), struct.getLong("days"),
                struct.getLong("seconds"), struct.getInt("nanos"));
    }

    private static Value readPoint(StructAccessor struct) {
        int srid = struct.getInt("srid");
        double x = struct.getDouble("x");
        double y = struct.getDouble("y");
        if (struct.isNull("z")) {
            return Values.point(srid, x, y);
        }
        return Values.point(srid, x, y, struct.getDouble("z"));
    }
}
