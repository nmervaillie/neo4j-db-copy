package org.neo4j.dbcopy.schema;

/**
 * A Neo4j property type, as carried by the parquet export.
 */
public sealed interface PropertyType permits PropertyType.Scalar, PropertyType.ListOf {

    enum Scalar implements PropertyType {
        BOOLEAN,
        INTEGER,
        FLOAT,
        STRING,
        BYTE_ARRAY,
        DATE,
        LOCAL_TIME,
        OFFSET_TIME,
        LOCAL_DATE_TIME,
        DATE_TIME,
        DURATION,
        POINT,
        /**
         * The element type of a list that was empty in every sampled row. Neo4j's empty list is
         * untyped, so there is nothing to infer. Never emitted to parquet: it renders as STRING.
         */
        UNKNOWN
    }

    record ListOf(PropertyType element) implements PropertyType {
    }
}
