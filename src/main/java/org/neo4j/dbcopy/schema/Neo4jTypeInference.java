package org.neo4j.dbcopy.schema;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * The single type-dispatch point of the parquet export. The schema scan, the writer and the
 * reader all go through this class, so they cannot disagree about what a type is.
 */
public final class Neo4jTypeInference {

    private static final String LIST_PREFIX = "LIST<";

    private Neo4jTypeInference() {
    }

    public static PropertyType inferType(Value value) {
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Cannot infer a property type from a null value.");
        }
        Object o = value.asObject();
        if (o instanceof Boolean) return PropertyType.Scalar.BOOLEAN;
        if (o instanceof Long) return PropertyType.Scalar.INTEGER;
        if (o instanceof Double) return PropertyType.Scalar.FLOAT;
        if (o instanceof String) return PropertyType.Scalar.STRING;
        if (o instanceof byte[]) return PropertyType.Scalar.BYTE_ARRAY;
        if (o instanceof LocalDate) return PropertyType.Scalar.DATE;
        if (o instanceof LocalTime) return PropertyType.Scalar.LOCAL_TIME;
        if (o instanceof OffsetTime) return PropertyType.Scalar.OFFSET_TIME;
        if (o instanceof LocalDateTime) return PropertyType.Scalar.LOCAL_DATE_TIME;
        if (o instanceof ZonedDateTime || o instanceof OffsetDateTime) return PropertyType.Scalar.DATE_TIME;
        if (o instanceof IsoDuration) return PropertyType.Scalar.DURATION;
        if (o instanceof Point) return PropertyType.Scalar.POINT;
        if (o instanceof List<?> list) return inferListType(list);
        throw unsupported(o);
    }

    private static PropertyType inferListType(List<?> list) {
        PropertyType element = null;
        for (Object e : list) {
            PropertyType observed = inferType(Values.value(e));
            element = element == null ? observed : merge(element, observed);
        }
        return new PropertyType.ListOf(element == null ? PropertyType.Scalar.UNKNOWN : element);
    }

    public static PropertyType merge(PropertyType existing, PropertyType observed) {
        if (existing.equals(observed)) {
            return existing;
        }
        if (existing == PropertyType.Scalar.UNKNOWN) {
            return observed;
        }
        if (observed == PropertyType.Scalar.UNKNOWN) {
            return existing;
        }
        if (existing instanceof PropertyType.ListOf a && observed instanceof PropertyType.ListOf b) {
            return new PropertyType.ListOf(merge(a.element(), b.element()));
        }
        throw new IllegalArgumentException(
                "Conflicting property types " + toToken(existing) + " and " + toToken(observed) + ".");
    }

    public static String toToken(PropertyType type) {
        if (type instanceof PropertyType.ListOf list) {
            PropertyType element = list.element() == PropertyType.Scalar.UNKNOWN
                    ? PropertyType.Scalar.STRING
                    : list.element();
            return LIST_PREFIX + toToken(element) + ">";
        }
        PropertyType.Scalar scalar = (PropertyType.Scalar) type;
        if (scalar == PropertyType.Scalar.UNKNOWN) {
            throw new IllegalArgumentException("UNKNOWN is only valid as a list element type.");
        }
        return scalar.name();
    }

    public static PropertyType fromToken(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Blank property type token.");
        }
        if (token.startsWith(LIST_PREFIX) && token.endsWith(">")) {
            return new PropertyType.ListOf(
                    fromToken(token.substring(LIST_PREFIX.length(), token.length() - 1)));
        }
        try {
            PropertyType.Scalar scalar = PropertyType.Scalar.valueOf(token);
            if (scalar == PropertyType.Scalar.UNKNOWN) {
                throw new IllegalArgumentException("Unsupported property type token '" + token + "'.");
            }
            return scalar;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported property type token '" + token + "'.", e);
        }
    }

    private static IllegalArgumentException unsupported(Object value) {
        String typeName = value == null ? "null" : value.getClass().getName();
        if (value instanceof Map<?, ?>) {
            return new IllegalArgumentException("Unsupported property value type for parquet export: "
                    + typeName + ". Neo4j property maps are not supported.");
        }
        return new IllegalArgumentException(
                "Unsupported property value type for parquet export: " + typeName);
    }
}
