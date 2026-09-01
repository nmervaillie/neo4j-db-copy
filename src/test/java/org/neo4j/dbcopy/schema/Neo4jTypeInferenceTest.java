package org.neo4j.dbcopy.schema;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.*;

class Neo4jTypeInferenceTest {

    @Test
    void should_infer_every_supported_scalar_type() {
        assertThat(Neo4jTypeInference.inferType(Values.value(true))).isEqualTo(BOOLEAN);
        assertThat(Neo4jTypeInference.inferType(Values.value(42L))).isEqualTo(INTEGER);
        assertThat(Neo4jTypeInference.inferType(Values.value(2.5d))).isEqualTo(FLOAT);
        assertThat(Neo4jTypeInference.inferType(Values.value("a"))).isEqualTo(STRING);
        assertThat(Neo4jTypeInference.inferType(Values.value(new byte[]{1}))).isEqualTo(BYTE_ARRAY);
        assertThat(Neo4jTypeInference.inferType(Values.value(LocalDate.of(2024, 1, 1)))).isEqualTo(DATE);
        assertThat(Neo4jTypeInference.inferType(Values.value(LocalTime.NOON))).isEqualTo(LOCAL_TIME);
        assertThat(Neo4jTypeInference.inferType(Values.value(OffsetTime.of(LocalTime.NOON, ZoneOffset.UTC))))
                .isEqualTo(OFFSET_TIME);
        assertThat(Neo4jTypeInference.inferType(Values.value(LocalDateTime.of(2024, 1, 1, 12, 0))))
                .isEqualTo(LOCAL_DATE_TIME);
        assertThat(Neo4jTypeInference.inferType(Values.value(ZonedDateTime.parse("2024-01-01T12:00+01:00[Europe/Paris]"))))
                .isEqualTo(DATE_TIME);
        assertThat(Neo4jTypeInference.inferType(Values.value(OffsetDateTime.parse("2024-01-01T12:00Z"))))
                .isEqualTo(DATE_TIME);
        assertThat(Neo4jTypeInference.inferType(Values.isoDuration(1, 2, 3, 4))).isEqualTo(DURATION);
        assertThat(Neo4jTypeInference.inferType(Values.point(4326, 2.35, 48.85))).isEqualTo(POINT);
    }

    @Test
    void should_infer_list_element_types() {
        assertThat(Neo4jTypeInference.inferType(Values.value(List.of("a", "b"))))
                .isEqualTo(new PropertyType.ListOf(STRING));
        assertThat(Neo4jTypeInference.inferType(Values.value(List.of(1L, 2L))))
                .isEqualTo(new PropertyType.ListOf(INTEGER));
    }

    @Test
    void should_treat_an_empty_list_element_type_as_unknown() {
        assertThat(Neo4jTypeInference.inferType(Values.value(List.of())))
                .isEqualTo(new PropertyType.ListOf(UNKNOWN));
    }

    @Test
    void should_let_a_real_element_type_win_over_the_unknown_placeholder() {
        PropertyType empty = new PropertyType.ListOf(UNKNOWN);
        PropertyType integers = new PropertyType.ListOf(INTEGER);

        assertThat(Neo4jTypeInference.merge(empty, integers)).isEqualTo(integers);
        assertThat(Neo4jTypeInference.merge(integers, empty)).isEqualTo(integers);
    }

    @Test
    void should_merge_identical_types_to_themselves() {
        assertThat(Neo4jTypeInference.merge(STRING, STRING)).isEqualTo(STRING);
    }

    @Test
    void should_reject_conflicting_types() {
        assertThatThrownBy(() -> Neo4jTypeInference.merge(INTEGER, STRING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("INTEGER")
                .hasMessageContaining("STRING");
    }

    @Test
    void should_reject_unsupported_values() {
        assertThatThrownBy(() -> Neo4jTypeInference.inferType(Values.NULL))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void should_round_trip_footer_tokens() {
        assertThat(Neo4jTypeInference.toToken(STRING)).isEqualTo("STRING");
        assertThat(Neo4jTypeInference.toToken(new PropertyType.ListOf(INTEGER))).isEqualTo("LIST<INTEGER>");
        assertThat(Neo4jTypeInference.fromToken("STRING")).isEqualTo(STRING);
        assertThat(Neo4jTypeInference.fromToken("LIST<INTEGER>")).isEqualTo(new PropertyType.ListOf(INTEGER));
    }

    @Test
    void should_render_an_unknown_list_element_as_string() {
        assertThat(Neo4jTypeInference.toToken(new PropertyType.ListOf(UNKNOWN))).isEqualTo("LIST<STRING>");
    }

    @Test
    void should_reject_an_unparseable_token() {
        assertThatThrownBy(() -> Neo4jTypeInference.fromToken("MAP<STRING>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MAP<STRING>");
    }
}
