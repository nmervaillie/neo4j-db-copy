# Parquet Import/Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Parquet as a third endpoint kind alongside Bolt and JSONL, usable as both source and target, with a Neo4j -> Parquet -> Neo4j round trip that preserves every property type exactly.

**Architecture:** A new `schema` package models Neo4j property types and owns the single type-dispatch point (`Neo4jTypeInference`). A `BoltSchemaScanner` samples the source graph to build a `GraphSchema`, which the CLI hands to `ParquetDataWriter` before the first row is written. The writer emits one Parquet file per node label set and per relationship type, stamping the Neo4j type map into each file's Parquet footer; `ParquetDataReader` reads those footers back so import never infers types from physical layout.

**Tech Stack:** Java 21, `dev.hardwood:hardwood-core:1.1.0.Beta1`, Neo4j Java Driver 6.0.4, Project Reactor, picocli, JUnit 5 + AssertJ, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-01-parquet-import-export-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **Java baseline is 21.** Hardwood requires 21+. Do not use language features above 21.
- **Parquet library:** `dev.hardwood:hardwood-core`, version managed by `dev.hardwood:hardwood-bom:1.1.0.Beta1`. No other Parquet library.
- **Reserved column names, exact spelling:** `__dbcopy_id`, `__dbcopy_labels`, `__dbcopy_source_id`, `__dbcopy_target_id`, `__dbcopy_type`. Reserved prefix is `__dbcopy_`.
- **Node identity is `id()`**, not `elementId()` — matching the JSONL export.
- **All property columns are `OPTIONAL`.** Only reserved identity columns are `REQUIRED`.
- **Scan sample size is a constant**, `1000`, exposed as `CopyOptions.schemaSampleSize()`. No CLI flag in this plan.
- **Type mismatches fail fast** with a message naming entity, property, expected type and actual type. Never coerce, never skip.
- **Test conventions:** JUnit 5, AssertJ (`assertThat`, `assertThatThrownBy`), test methods named `should_snake_case`. Unit tests are `*Test`, integration tests are `*IT` (Failsafe).
- **Compression codec:** `CompressionCodec.ZSTD`.

## Type mapping reference

Used by Tasks 2, 4, 6 and 11. Copy exactly.

| `PropertyType` | Parquet |
|---|---|
| `BOOLEAN` | `BOOLEAN` |
| `INTEGER` | `INT64` |
| `FLOAT` | `DOUBLE` |
| `STRING` | `BYTE_ARRAY` + `LogicalType.StringType` |
| `BYTE_ARRAY` | `BYTE_ARRAY`, no logical type |
| `DATE` | `INT32` + `LogicalType.DateType` |
| `LOCAL_TIME` | `INT64` + `LogicalType.TimeType(false, NANOS)` |
| `OFFSET_TIME` | `STRUCT{nanoOfDay INT64 + TimeType(false,NANOS) REQUIRED, offsetSeconds INT32 REQUIRED}` |
| `LOCAL_DATE_TIME` | `STRUCT{epochSecond INT64 REQUIRED, nano INT32 REQUIRED}` |
| `DATE_TIME` | `STRUCT{epochSecond INT64 REQUIRED, nano INT32 REQUIRED, zoneId STRING OPTIONAL, offsetSeconds INT32 REQUIRED}` |
| `DURATION` | `STRUCT{months INT64 REQUIRED, days INT64 REQUIRED, seconds INT64 REQUIRED, nanos INT32 REQUIRED}` |
| `POINT` | `STRUCT{srid INT32 REQUIRED, x DOUBLE REQUIRED, y DOUBLE REQUIRED, z DOUBLE OPTIONAL}` |
| `ListOf(T)` | `LIST<T>` with element repetition `REQUIRED` |
| `UNKNOWN` | never emitted; treated as `STRING` (see Task 2) |

## File structure

**Create:**

| File | Responsibility |
|---|---|
| `src/main/java/org/neo4j/dbcopy/schema/PropertyType.java` | Sealed Neo4j property type model |
| `src/main/java/org/neo4j/dbcopy/schema/Neo4jTypeInference.java` | `Value` <-> `PropertyType`, merge, footer tokens |
| `src/main/java/org/neo4j/dbcopy/schema/GraphSchema.java` | `GraphSchema`, `NodeTypeSchema`, `RelationshipTypeSchema` records |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetLayout.java` | Reserved names, file naming, directory validation |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapper.java` | `GraphSchema` -> hardwood `FileSchema` |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetFooter.java` | Read/write the `neo4j.*` footer keys |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java` | Per-type row setters and getters, both directions |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataWriter.java` | `DataWriter` |
| `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataReader.java` | `DataReader` |
| `src/main/java/org/neo4j/dbcopy/bolt/BoltSchemaScanner.java` | Cypher sampling scan |
| `src/main/java/org/neo4j/dbcopy/io/NodeRecord.java` | Shared `Node` implementation |
| `src/main/java/org/neo4j/dbcopy/io/RelationshipRecord.java` | Shared `Relationship` implementation |

**Modify:** `pom.xml`, `.github/workflows/ci.yml`, `README.adoc`, `CopyOptions.java`, `DbCopyCli.java`, `io/DataReader.java`, `io/DataReaderFactory.java`, `io/DataWriterFactory.java`, `io/EndpointFactorySupport.java`, `bolt/BoltReader.java`, `json/JsonDataReader.java`.

**Shippable midpoint:** after Task 9 the export path is complete and releasable. Import starts at Task 10.

---

### Task 1: Java 21 baseline and hardwood dependency

**Files:**
- Modify: `pom.xml`
- Modify: `.github/workflows/ci.yml:19-24`
- Modify: `README.adoc:16`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/HardwoodSmokeTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: hardwood on the compile classpath; Java 21 language level.

- [ ] **Step 1: Write the failing test**

This test exists to prove the dependency and the Java level actually work end to end, not to test our code. Create `src/test/java/org/neo4j/dbcopy/parquet/HardwoodSmokeTest.java`:

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.OutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class HardwoodSmokeTest {

    @Test
    void should_write_and_read_a_parquet_file(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("smoke.parquet");
        FileSchema schema = FileSchema.builder("smoke")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        new LogicalType.StringType())
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setLong("id", 1L).setString("name", "Ada"));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(1L);
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getString("name")).isEqualTo("Ada");
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=HardwoodSmokeTest`
Expected: FAIL — compilation error, `package dev.hardwood does not exist`.

- [ ] **Step 3: Add the dependency and raise the Java level**

In `pom.xml` `<properties>`, change three values (the block is kept alphabetically sorted by the sortpom plugin, so insert `hardwood.version` in order):

```xml
<hardwood.version>1.1.0.Beta1</hardwood.version>
<java.version>21</java.version>
<maven.compiler.source>21</maven.compiler.source>
<maven.compiler.target>21</maven.compiler.target>
```

In `<dependencyManagement><dependencies>`, add the BOM alongside the existing reactor and testcontainers BOMs:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-bom</artifactId>
    <version>${hardwood.version}</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

In `<dependencies>`, add:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
    <exclusions>
        <exclusion>
            <groupId>com.aayushatharva.brotli4j</groupId>
            <artifactId>brotli4j</artifactId>
        </exclusion>
        <exclusion>
            <groupId>at.yawk.lz4</groupId>
            <artifactId>lz4-java</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

The exclusions drop native codec libraries we never use, keeping the shaded executable jar small. `zstd-jni` and `snappy-java` stay — ZSTD is the codec we write with.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=HardwoodSmokeTest`
Expected: PASS.

- [ ] **Step 5: Verify the whole existing suite still compiles and passes on Java 21**

Run: `mvn -q test`
Expected: PASS, no failures. If the build reports a release-version error, the local JDK is below 21 — check with `java -version` and switch JDK before continuing.

- [ ] **Step 6: Update CI and README**

In `.github/workflows/ci.yml`, replace the JDK setup step:

```yaml
      - name: Set up JDK 21
        uses: actions/setup-java@v3
        with:
          distribution: 'temurin'
          java-version: '21'
```

In `README.adoc`, replace the line `Building / running requires Java 17+.` with:

```adoc
Building / running requires Java 21+.
```

- [ ] **Step 7: Commit**

```bash
git add pom.xml .github/workflows/ci.yml README.adoc \
        src/test/java/org/neo4j/dbcopy/parquet/HardwoodSmokeTest.java
git commit -m "build: Require Java 21 and add hardwood parquet dependency"
```

---

### Task 2: Property type model and type inference

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/schema/PropertyType.java`
- Create: `src/main/java/org/neo4j/dbcopy/schema/Neo4jTypeInference.java`
- Test: `src/test/java/org/neo4j/dbcopy/schema/Neo4jTypeInferenceTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `sealed interface PropertyType permits PropertyType.Scalar, PropertyType.ListOf`
  - `enum PropertyType.Scalar implements PropertyType { BOOLEAN, INTEGER, FLOAT, STRING, BYTE_ARRAY, DATE, LOCAL_TIME, OFFSET_TIME, LOCAL_DATE_TIME, DATE_TIME, DURATION, POINT, UNKNOWN }`
  - `record PropertyType.ListOf(PropertyType element) implements PropertyType`
  - `static PropertyType Neo4jTypeInference.inferType(Value value)`
  - `static PropertyType Neo4jTypeInference.merge(PropertyType existing, PropertyType observed)`
  - `static String Neo4jTypeInference.toToken(PropertyType type)`
  - `static PropertyType Neo4jTypeInference.fromToken(String token)`

**Why `UNKNOWN` exists:** Neo4j's `[]` is untyped, so an all-empty-list sample has no element type. `UNKNOWN` is that placeholder. `merge(UNKNOWN, x) == x`, so a sample of `[]` followed by `[1, 2]` resolves to `LIST<INTEGER>` rather than falsely conflicting. It never reaches Parquet: `toToken(ListOf(UNKNOWN))` renders `LIST<STRING>`, and `fromToken` never produces it.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/neo4j/dbcopy/schema/Neo4jTypeInferenceTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=Neo4jTypeInferenceTest`
Expected: FAIL — `package org.neo4j.dbcopy.schema does not exist`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/schema/PropertyType.java`:

```java
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
```

Create `src/main/java/org/neo4j/dbcopy/schema/Neo4jTypeInference.java`:

```java
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=Neo4jTypeInferenceTest`
Expected: PASS, 10 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/schema \
        src/test/java/org/neo4j/dbcopy/schema
git commit -m "feat: Add Neo4j property type model and type inference"
```

---

### Task 3: Graph schema records and parquet layout

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/schema/GraphSchema.java`
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetLayout.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetLayoutTest.java`

**Interfaces:**
- Consumes: `PropertyType` (Task 2).
- Produces:
  - `record GraphSchema(List<NodeTypeSchema> nodeTypes, List<RelationshipTypeSchema> relTypes)`
  - `record NodeTypeSchema(SortedSet<String> labels, SortedMap<String, PropertyType> properties)`
  - `record RelationshipTypeSchema(String type, SortedMap<String, PropertyType> properties)`
  - `ParquetLayout.ID / LABELS / SOURCE_ID / TARGET_ID / TYPE / RESERVED_PREFIX / NODES_DIR / RELATIONSHIPS_DIR`
  - `static String ParquetLayout.nodeFileName(SortedSet<String> labels)`
  - `static String ParquetLayout.relationshipFileName(String type)`
  - `static void ParquetLayout.requireNoReservedProperties(String context, Collection<String> propertyNames)`
  - `static void ParquetLayout.requireDistinctFileNames(List<String> fileNames)`
  - `static void ParquetLayout.prepareExportDirectory(Path directory)`
  - `static void ParquetLayout.requireImportDirectory(Path directory)`

**Note on file naming:** labels are sorted and joined with `__`, and any character outside `[A-Za-z0-9_.-]` is percent-escaped so the name is a legal file name. `_` is deliberately *not* escaped, because it is common in labels and escaping it would make every name unreadable. That leaves one obscure collision — label set `{A__B}` and `{A, B}` both render `A__B.parquet` — which `requireDistinctFileNames` catches with a clear error rather than silently merging two files.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetLayoutTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetLayoutTest`
Expected: FAIL — `cannot find symbol: class ParquetLayout`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/schema/GraphSchema.java` (three records in one file — they are a single cohesive data model and always change together):

```java
package org.neo4j.dbcopy.schema;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * The inferred shape of a graph: one entry per distinct node label set, one per relationship type.
 */
public record GraphSchema(List<NodeTypeSchema> nodeTypes, List<RelationshipTypeSchema> relTypes) {

    public GraphSchema {
        nodeTypes = List.copyOf(nodeTypes);
        relTypes = List.copyOf(relTypes);
    }

    public record NodeTypeSchema(SortedSet<String> labels, SortedMap<String, PropertyType> properties) {
    }

    public record RelationshipTypeSchema(String type, SortedMap<String, PropertyType> properties) {
    }
}
```

Note the nested placement: later tasks refer to these as `GraphSchema.NodeTypeSchema` and
`GraphSchema.RelationshipTypeSchema`, or import them directly.

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetLayout.java`:

```java
package org.neo4j.dbcopy.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Naming and directory rules for a parquet export: which columns are reserved, which file a
 * label set or relationship type lands in, and what a valid export directory looks like.
 */
public final class ParquetLayout {

    public static final String RESERVED_PREFIX = "__dbcopy_";
    public static final String ID = RESERVED_PREFIX + "id";
    public static final String LABELS = RESERVED_PREFIX + "labels";
    public static final String SOURCE_ID = RESERVED_PREFIX + "source_id";
    public static final String TARGET_ID = RESERVED_PREFIX + "target_id";
    public static final String TYPE = RESERVED_PREFIX + "type";

    public static final String NODES_DIR = "nodes";
    public static final String RELATIONSHIPS_DIR = "relationships";

    private static final String NO_LABELS = "_no_labels";
    private static final String EXTENSION = ".parquet";
    private static final String LABEL_SEPARATOR = "__";

    private ParquetLayout() {
    }

    public static String nodeFileName(SortedSet<String> labels) {
        if (labels.isEmpty()) {
            return NO_LABELS + EXTENSION;
        }
        return labels.stream().map(ParquetLayout::escape).collect(Collectors.joining(LABEL_SEPARATOR))
                + EXTENSION;
    }

    public static String relationshipFileName(String type) {
        return escape(type) + EXTENSION;
    }

    public static void requireNoReservedProperties(String context, Collection<String> propertyNames) {
        List<String> reserved = propertyNames.stream()
                .filter(name -> name.startsWith(RESERVED_PREFIX))
                .sorted()
                .toList();
        if (!reserved.isEmpty()) {
            throw new IllegalArgumentException("Parquet export failed: " + context + " has properties "
                    + reserved + ", which collide with the reserved '" + RESERVED_PREFIX
                    + "' identity columns. Rename or exclude them.");
        }
    }

    public static void requireDistinctFileNames(List<String> fileNames) {
        Set<String> seen = new HashSet<>();
        for (String fileName : fileNames) {
            if (!seen.add(fileName)) {
                throw new IllegalArgumentException("Parquet export failed: two label sets or "
                        + "relationship types both map to the file name '" + fileName
                        + "'. Rename one of them or exclude it from the copy.");
            }
        }
    }

    public static void prepareExportDirectory(Path directory) {
        try {
            if (Files.exists(directory)) {
                if (!Files.isDirectory(directory)) {
                    throw new IllegalArgumentException(
                            "The parquet export target must be a directory: " + directory);
                }
                try (Stream<Path> entries = Files.list(directory)) {
                    if (entries.findAny().isPresent()) {
                        throw new IllegalArgumentException(
                                "Refusing to write into non-empty parquet export directory: " + directory
                                        + ". Please remove it first or choose a different target path.");
                    }
                }
            }
            Files.createDirectories(directory.resolve(NODES_DIR));
            Files.createDirectories(directory.resolve(RELATIONSHIPS_DIR));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to prepare parquet export directory " + directory, e);
        }
    }

    public static void requireImportDirectory(Path directory) {
        if (!Files.exists(directory)) {
            throw new IllegalArgumentException("Parquet import directory does not exist: " + directory);
        }
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException(
                    "The parquet import source must be a directory: " + directory);
        }
        boolean hasNodes = Files.isDirectory(directory.resolve(NODES_DIR));
        boolean hasRelationships = Files.isDirectory(directory.resolve(RELATIONSHIPS_DIR));
        if (!hasNodes && !hasRelationships) {
            throw new IllegalArgumentException(directory + " is not a neo4j-db-copy parquet export: "
                    + "it contains neither a '" + NODES_DIR + "' nor a '" + RELATIONSHIPS_DIR
                    + "' directory.");
        }
    }

    /**
     * Percent-escapes anything that is not safe in a file name. '_' is left alone on purpose: it is
     * common in labels, and escaping it would make every file name unreadable. The resulting
     * ambiguity with the '__' label separator is caught by {@link #requireDistinctFileNames}.
     */
    private static String escape(String name) {
        StringBuilder escaped = new StringBuilder(name.length());
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) (b & 0xFF);
            boolean safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.';
            if (safe) {
                escaped.append(c);
            } else {
                escaped.append('%').append(String.format("%02X", b & 0xFF));
            }
        }
        return escaped.toString();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetLayoutTest`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/schema/GraphSchema.java \
        src/main/java/org/neo4j/dbcopy/parquet/ParquetLayout.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetLayoutTest.java
git commit -m "feat: Add graph schema records and parquet layout rules"
```

---

### Task 4: Schema mapper

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapper.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapperTest.java`

**Interfaces:**
- Consumes: `PropertyType` (Task 2), `GraphSchema.NodeTypeSchema`, `GraphSchema.RelationshipTypeSchema`, `ParquetLayout` constants (Task 3).
- Produces:
  - `static FileSchema ParquetSchemaMapper.nodeFileSchema(NodeTypeSchema nodeType)`
  - `static FileSchema ParquetSchemaMapper.relationshipFileSchema(RelationshipTypeSchema relType)`

Follow the type mapping reference table at the top of this plan exactly.

- [ ] **Step 1: Write the failing test**

The assertions read the resulting `FileSchema` through `FileSchema.getColumns()`, which returns
`List<ColumnSchema>`; each `ColumnSchema` exposes `path()` (dotted for nested fields),
`physicalType()`, `repetitionType()` and `logicalType()`. Confirm those accessor names first with:

```bash
javap -cp ~/.m2/repository/dev/hardwood/hardwood-core/1.1.0.Beta1/hardwood-core-1.1.0.Beta1.jar \
      dev.hardwood.schema.FileSchema dev.hardwood.schema.ColumnSchema
```

If the accessors differ, adjust the helper below only — the expected values stay the same.

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapperTest.java`:

```java
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

        assertThat(paths(schema)).startsWith("__dbcopy_id", "__dbcopy_labels");
        assertThat(column(schema, "__dbcopy_id").physicalType()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "__dbcopy_id").repetitionType()).isEqualTo(RepetitionType.REQUIRED);
    }

    @Test
    void should_map_reserved_relationship_columns() {
        FileSchema schema = ParquetSchemaMapper.relationshipFileSchema(
                new RelationshipTypeSchema("KNOWS", new TreeMap<>()));

        assertThat(paths(schema)).startsWith(
                "__dbcopy_id", "__dbcopy_source_id", "__dbcopy_target_id", "__dbcopy_type");
        assertThat(column(schema, "__dbcopy_source_id").physicalType()).isEqualTo(PhysicalType.INT64);
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

        assertThat(column(schema, "active").physicalType()).isEqualTo(PhysicalType.BOOLEAN);
        assertThat(column(schema, "age").physicalType()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "ratio").physicalType()).isEqualTo(PhysicalType.DOUBLE);
        assertThat(column(schema, "name").logicalType()).isEqualTo(new LogicalType.StringType());
        assertThat(column(schema, "payload").physicalType()).isEqualTo(PhysicalType.BYTE_ARRAY);
        assertThat(column(schema, "payload").logicalType()).isNull();
        assertThat(column(schema, "born").physicalType()).isEqualTo(PhysicalType.INT32);
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
        assertThat(column(schema, "seen.epochSecond").physicalType()).isEqualTo(PhysicalType.INT64);
        assertThat(column(schema, "seen.nano").physicalType()).isEqualTo(PhysicalType.INT32);
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
    }

    @Test
    void should_map_an_unknown_list_element_as_a_string() {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        properties.put("empties", new PropertyType.ListOf(UNKNOWN));

        FileSchema schema = ParquetSchemaMapper.nodeFileSchema(nodeType(properties));

        assertThat(paths(schema)).anyMatch(path -> path.startsWith("empties"));
    }

    private static NodeTypeSchema nodeType(SortedMap<String, PropertyType> properties) {
        return new NodeTypeSchema(new TreeSet<>(List.of("Person")), properties);
    }

    private static List<String> paths(FileSchema schema) {
        return schema.getColumns().stream().map(ColumnSchema::path).toList();
    }

    private static ColumnSchema column(FileSchema schema, String path) {
        return schema.getColumns().stream()
                .filter(c -> c.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column at path '" + path + "' in " + paths(schema)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetSchemaMapperTest`
Expected: FAIL — `cannot find symbol: class ParquetSchemaMapper`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapper.java`:

```java
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
                    e -> addListElement(e, resolve(list.element())));
            return;
        }
        addScalar(builder, name, resolveScalar(type));
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
    private static PropertyType.Scalar resolveScalar(PropertyType type) {
        if (!(type instanceof PropertyType.Scalar scalar)) {
            throw new IllegalArgumentException(
                    "Nested lists are not supported as Neo4j properties: " + type);
        }
        return resolve(scalar);
    }

    private static PropertyType.Scalar resolve(PropertyType type) {
        PropertyType.Scalar scalar = resolveIfScalar(type);
        return scalar == PropertyType.Scalar.UNKNOWN ? PropertyType.Scalar.STRING : scalar;
    }

    private static PropertyType.Scalar resolveIfScalar(PropertyType type) {
        if (type instanceof PropertyType.Scalar scalar) {
            return scalar;
        }
        throw new IllegalArgumentException(
                "Nested lists are not supported as Neo4j properties: " + type);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetSchemaMapperTest`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapper.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetSchemaMapperTest.java
git commit -m "feat: Map graph schema to parquet file schemas"
```

---

### Task 5: Footer metadata

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetFooter.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetFooterTest.java`

**Interfaces:**
- Consumes: `PropertyType`, `Neo4jTypeInference.toToken`/`fromToken` (Task 2).
- Produces:
  - `record ParquetFooter(String recordType, List<String> labels, String relationshipType, SortedMap<String, PropertyType> types)`
  - `static final String ParquetFooter.VERSION = "1"`
  - `static final String ParquetFooter.RECORD_TYPE_NODE = "node"`
  - `static final String ParquetFooter.RECORD_TYPE_RELATIONSHIP = "relationship"`
  - `static ParquetFooter ParquetFooter.forNode(SortedSet<String> labels, SortedMap<String, PropertyType> types)`
  - `static ParquetFooter ParquetFooter.forRelationship(String type, SortedMap<String, PropertyType> types)`
  - `Map<String, String> toKeyValueMetadata()`
  - `static ParquetFooter fromKeyValueMetadata(Map<String, String> metadata, Path file)`

This is what makes import exact: it records the declared Neo4j type of every property, so the
reader never infers a type from the physical parquet layout. Serialised with Jackson, already a
project dependency.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetFooterTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetFooterTest`
Expected: FAIL — `cannot find symbol: class ParquetFooter`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetFooter.java`:

```java
package org.neo4j.dbcopy.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * The {@code neo4j.*} keys stamped into each parquet file's footer. They record the declared Neo4j
 * type of every property, so import never has to infer a type from the physical parquet layout.
 */
public record ParquetFooter(String recordType,
                            List<String> labels,
                            String relationshipType,
                            SortedMap<String, PropertyType> types) {

    public static final String VERSION = "1";
    public static final String RECORD_TYPE_NODE = "node";
    public static final String RECORD_TYPE_RELATIONSHIP = "relationship";

    private static final String KEY_VERSION = "neo4j.dbcopy.version";
    private static final String KEY_RECORD_TYPE = "neo4j.recordType";
    private static final String KEY_LABELS = "neo4j.labels";
    private static final String KEY_RELATIONSHIP_TYPE = "neo4j.relationshipType";
    private static final String KEY_TYPES = "neo4j.types";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ParquetFooter forNode(SortedSet<String> labels,
                                        SortedMap<String, PropertyType> types) {
        return new ParquetFooter(RECORD_TYPE_NODE, List.copyOf(labels), null, types);
    }

    public static ParquetFooter forRelationship(String relationshipType,
                                                SortedMap<String, PropertyType> types) {
        return new ParquetFooter(RECORD_TYPE_RELATIONSHIP, List.of(), relationshipType, types);
    }

    public Map<String, String> toKeyValueMetadata() {
        Map<String, String> tokens = new LinkedHashMap<>();
        types.forEach((name, type) -> tokens.put(name, Neo4jTypeInference.toToken(type)));

        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(KEY_VERSION, VERSION);
        metadata.put(KEY_RECORD_TYPE, recordType);
        if (RECORD_TYPE_NODE.equals(recordType)) {
            metadata.put(KEY_LABELS, writeJson(labels));
        } else {
            metadata.put(KEY_RELATIONSHIP_TYPE, relationshipType);
        }
        metadata.put(KEY_TYPES, writeJson(tokens));
        return metadata;
    }

    public static ParquetFooter fromKeyValueMetadata(Map<String, String> metadata, Path file) {
        String version = metadata.get(KEY_VERSION);
        if (version == null) {
            throw new IllegalArgumentException(file + " is not a neo4j-db-copy parquet export: "
                    + "its footer has no '" + KEY_VERSION + "' entry.");
        }
        if (!VERSION.equals(version)) {
            throw new IllegalArgumentException(file + " was written by an incompatible version of "
                    + "neo4j-db-copy (footer version " + version + ", this build reads version "
                    + VERSION + ").");
        }

        String recordType = metadata.get(KEY_RECORD_TYPE);
        SortedMap<String, PropertyType> types = readTypes(metadata.get(KEY_TYPES), file);

        if (RECORD_TYPE_NODE.equals(recordType)) {
            List<String> labels = readJson(metadata.get(KEY_LABELS), new TypeReference<>() {
            }, file);
            return new ParquetFooter(RECORD_TYPE_NODE, labels, null, types);
        }
        if (RECORD_TYPE_RELATIONSHIP.equals(recordType)) {
            return new ParquetFooter(RECORD_TYPE_RELATIONSHIP, List.of(),
                    metadata.get(KEY_RELATIONSHIP_TYPE), types);
        }
        throw new IllegalArgumentException(
                "Unsupported '" + KEY_RECORD_TYPE + "' value '" + recordType + "' in " + file);
    }

    private static SortedMap<String, PropertyType> readTypes(String json, Path file) {
        Map<String, String> tokens = readJson(json, new TypeReference<>() {
        }, file);
        SortedMap<String, PropertyType> types = new TreeMap<>();
        tokens.forEach((name, token) -> types.put(name, Neo4jTypeInference.fromToken(token)));
        return types;
    }

    private static String writeJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to serialise parquet footer metadata", e);
        }
    }

    private static <T> T readJson(String json, TypeReference<T> type, Path file) {
        if (json == null) {
            throw new IllegalArgumentException("Incomplete neo4j-db-copy footer in " + file);
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unreadable neo4j-db-copy footer in " + file, e);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetFooterTest`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/parquet/ParquetFooter.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetFooterTest.java
git commit -m "feat: Stamp Neo4j property types into the parquet footer"
```

---

### Task 6: Value setters (write side)

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetValuesWriteTest.java`

**Interfaces:**
- Consumes: `PropertyType`, `Neo4jTypeInference` (Task 2), `ParquetSchemaMapper` (Task 4).
- Produces:
  - `static void ParquetValues.set(StructBuilder row, String field, PropertyType type, Value value)`

Task 12 adds the matching `read` method to this same class. Keeping both directions in one file
is deliberate: a change to the struct layout of, say, `POINT` has to touch both methods, and they
are impossible to miss when they sit next to each other.

**Mismatch errors:** `set` throws `IllegalArgumentException` naming the property, the expected
type and the actual type. It does not know which node it is writing — `ParquetDataWriter`
(Task 7) catches and prefixes the entity context. The actual type is computed via
`Neo4jTypeInference.inferType` only on the error path, so the happy path stays allocation-free.

- [ ] **Step 1: Write the failing test**

The test writes one row through a real hardwood writer and reads it back, because that is the
only way to prove the setters agree with the schema the mapper produced.

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetValuesWriteTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetValuesWriteTest`
Expected: FAIL — `cannot find symbol: class ParquetValues`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java`:

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.writer.ListBuilder;
import dev.hardwood.writer.StructBuilder;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
}
```

Note `require` is called with `byte[].class` in one branch; `Class<byte[]>` works with the
generic signature as written.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetValuesWriteTest`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetValuesWriteTest.java
git commit -m "feat: Convert Neo4j property values to parquet row fields"
```

---

### Task 7: Parquet data writer

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataWriter.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java`

**Interfaces:**
- Consumes: `GraphSchema` (Task 3), `ParquetLayout` (Task 3), `ParquetSchemaMapper` (Task 4), `ParquetFooter` (Task 5), `ParquetValues.set` (Task 6), existing `DataWriter` and `CopyOptions`.
- Produces:
  - `public ParquetDataWriter(Path directory, GraphSchema schema)` implementing `DataWriter`

**Behaviour:**
- Constructor validates the whole schema up front: reserved-property collisions and duplicate file names, so a bad export fails before any file is created.
- Writers open lazily on first use, so a label set with no rows produces no file. Node writers are closed when relationship writing starts, keeping open handles close to the spec's sequential model.
- Every mismatch from `ParquetValues` is caught and re-thrown with the entity context prefixed.
- Excluded properties are filtered by `CopyOptions`, matching `BoltWriter.filterProperties`.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java`:

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.CopyOptions;
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
        Node person = TestEntities.node(1L, List.of("Person"), Map.of("name", Values.value("Ada")));
        Node employee = TestEntities.node(2L, List.of("Person", "Employee"),
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
        Node person = TestEntities.node(1L, List.of("Person"), Map.of("name", Values.value("Ada")));

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
        Relationship knows = TestEntities.relationship(7L, 1L, 2L, "KNOWS",
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
        Node person = TestEntities.node(1L, List.of("Person"), Map.of("name", Values.value("Ada")));

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
        Node person = TestEntities.node(1L, List.of("Person"), Map.of("name", Values.value("Ada")));
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
        Node person = TestEntities.node(4711L, List.of("Person"), Map.of("name", Values.value(1L)));

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
        Node ghost = TestEntities.node(9L, List.of("Ghost"), Map.of());

        assertThatThrownBy(() -> {
            try (ParquetDataWriter writer = new ParquetDataWriter(export, schema())) {
                writer.writeNodes(Flux.just(ghost), CopyOptions.DEFAULT).blockLast();
            }
        })
                .hasMessageContaining("Ghost")
                .hasMessageContaining("was not found in the scanned schema");
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
```

- [ ] **Step 2: Add the shared test entity helper**

`JsonDataReaderWriterTest` has private `TestNode`/`TestRelationship` classes that cannot be
reused from another package. Task 11 replaces both with production records. Until then, create
`src/test/java/org/neo4j/dbcopy/parquet/TestEntities.java` with the two factory methods the tests
above use. Copy the `TestNode` and `TestRelationship` bodies from
`src/test/java/org/neo4j/dbcopy/json/JsonDataReaderWriterTest.java` (they are at the bottom of
that file), make the classes package-private static nested classes of `TestEntities`, and expose:

```java
package org.neo4j.dbcopy.parquet;

import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;

final class TestEntities {

    private TestEntities() {
    }

    static Node node(long id, List<String> labels, Map<String, Value> properties) {
        return new TestNode(id, labels, properties);
    }

    static Relationship relationship(long id, long startId, long endId, String type,
                                     Map<String, Value> properties) {
        return new TestRelationship(id, startId, endId, type, properties);
    }

    // TestNode and TestRelationship copied from JsonDataReaderWriterTest; both are replaced by
    // io.NodeRecord and io.RelationshipRecord in Task 11.
}
```

Task 11 deletes this file and repoints the tests at the production records.

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetDataWriterTest`
Expected: FAIL — `cannot find symbol: class ParquetDataWriter`.

- [ ] **Step 4: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataWriter.java`:

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

/**
 * Writes a graph as one parquet file per node label set and per relationship type. The schema must
 * be known up front, which is why it is scanned from the source before this writer is created.
 */
public final class ParquetDataWriter implements DataWriter {

    private static final WriterConfig CONFIG = WriterConfig.builder()
            .codec(CompressionCodec.ZSTD)
            .build();

    private final Path directory;
    private final Map<String, NodeTypeSchema> nodeTypesByKey = new LinkedHashMap<>();
    private final Map<String, RelationshipTypeSchema> relTypesByType = new LinkedHashMap<>();
    private final Map<String, EntityWriter> nodeWriters = new LinkedHashMap<>();
    private final Map<String, EntityWriter> relWriters = new LinkedHashMap<>();

    public ParquetDataWriter(Path directory, GraphSchema schema) {
        this.directory = directory;
        validate(schema);
        for (NodeTypeSchema nodeType : schema.nodeTypes()) {
            nodeTypesByKey.put(labelKey(nodeType.labels()), nodeType);
        }
        for (RelationshipTypeSchema relType : schema.relTypes()) {
            relTypesByType.put(relType.type(), relType);
        }
        System.err.println("Writing parquet export to " + directory);
        ParquetLayout.prepareExportDirectory(directory);
    }

    private static void validate(GraphSchema schema) {
        List<String> fileNames = new ArrayList<>();
        for (NodeTypeSchema nodeType : schema.nodeTypes()) {
            ParquetLayout.requireNoReservedProperties(
                    "label set " + labelDescription(nodeType.labels()), nodeType.properties().keySet());
            fileNames.add(ParquetLayout.nodeFileName(nodeType.labels()));
        }
        ParquetLayout.requireDistinctFileNames(fileNames);

        List<String> relFileNames = new ArrayList<>();
        for (RelationshipTypeSchema relType : schema.relTypes()) {
            ParquetLayout.requireNoReservedProperties(
                    "relationship type " + relType.type(), relType.properties().keySet());
            relFileNames.add(ParquetLayout.relationshipFileName(relType.type()));
        }
        ParquetLayout.requireDistinctFileNames(relFileNames);
    }

    @Override
    public Flux<Long> writeNodes(Flux<Node> nodes, CopyOptions copyOptions) {
        return nodes.buffer(copyOptions.batchSize())
                .concatMap(batch -> Mono.fromCallable(() -> {
                    for (Node node : batch) {
                        writeNode(node, copyOptions.getNodePropertiesToExclude());
                    }
                    return (long) batch.size();
                }));
    }

    @Override
    public Flux<Long> writeRelationships(Flux<Relationship> relationships, CopyOptions copyOptions) {
        return relationships.buffer(copyOptions.batchSize())
                .concatMap(batch -> Mono.fromCallable(() -> {
                    for (Relationship relationship : batch) {
                        writeRelationship(relationship, copyOptions.getRelationshipPropertiesToExclude());
                    }
                    return (long) batch.size();
                }))
                .doFirst(this::closeNodeWriters);
    }

    @SuppressWarnings("deprecation")
    private void writeNode(Node node, Set<String> excluded) {
        SortedSet<String> labels = new TreeSet<>();
        node.labels().forEach(labels::add);
        NodeTypeSchema nodeType = nodeTypesByKey.get(labelKey(labels));
        if (nodeType == null) {
            throw new IllegalArgumentException("Parquet export failed: node " + node.id() + " "
                    + labelDescription(labels) + " was not found in the scanned schema. The source "
                    + "graph changed during the copy, or the scan sample missed this label set.");
        }
        EntityWriter writer = nodeWriters.computeIfAbsent(labelKey(labels), key -> EntityWriter.forNode(
                directory.resolve(ParquetLayout.NODES_DIR).resolve(ParquetLayout.nodeFileName(labels)),
                nodeType));

        String context = "node " + node.id() + " " + labelDescription(labels);
        writer.writeRow(context, row -> {
            row.setLong(ParquetLayout.ID, node.id());
            row.setList(ParquetLayout.LABELS, list -> labels.forEach(list::addString));
            setProperties(row, writer.types(), node::get, excluded);
        });
    }

    @SuppressWarnings("deprecation")
    private void writeRelationship(Relationship relationship, Set<String> excluded) {
        RelationshipTypeSchema relType = relTypesByType.get(relationship.type());
        if (relType == null) {
            throw new IllegalArgumentException("Parquet export failed: relationship "
                    + relationship.id() + " of type " + relationship.type()
                    + " was not found in the scanned schema. The source graph changed during the copy.");
        }
        EntityWriter writer = relWriters.computeIfAbsent(relationship.type(),
                type -> EntityWriter.forRelationship(directory.resolve(ParquetLayout.RELATIONSHIPS_DIR)
                        .resolve(ParquetLayout.relationshipFileName(type)), relType));

        String context = "relationship " + relationship.id() + " of type " + relationship.type();
        writer.writeRow(context, row -> {
            row.setLong(ParquetLayout.ID, relationship.id());
            row.setLong(ParquetLayout.SOURCE_ID, relationship.startNodeId());
            row.setLong(ParquetLayout.TARGET_ID, relationship.endNodeId());
            row.setString(ParquetLayout.TYPE, relationship.type());
            setProperties(row, writer.types(), relationship::get, excluded);
        });
    }

    private static void setProperties(dev.hardwood.writer.StructBuilder row,
                                      SortedMap<String, PropertyType> types,
                                      java.util.function.Function<String, Value> lookup,
                                      Set<String> excluded) {
        types.forEach((name, type) -> {
            Value value = excluded.contains(name) ? null : lookup.apply(name);
            ParquetValues.set(row, name, type, value);
        });
    }

    private static String labelKey(SortedSet<String> labels) {
        return String.join(" ", labels);
    }

    private static String labelDescription(Set<String> labels) {
        if (labels.isEmpty()) {
            return "(no labels)";
        }
        return "(:" + String.join(":", labels) + ")";
    }

    private void closeNodeWriters() {
        closeAll(nodeWriters);
    }

    @Override
    public void close() {
        closeNodeWriters();
        closeAll(relWriters);
    }

    private static void closeAll(Map<String, EntityWriter> writers) {
        RuntimeException failure = null;
        for (EntityWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        writers.clear();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * One open parquet file: its schema, its footer metadata and its row writer.
     */
    private record EntityWriter(ParquetFileWriter file,
                                RowWriter rows,
                                SortedMap<String, PropertyType> types,
                                Path path) {

        static EntityWriter forNode(Path path, NodeTypeSchema nodeType) {
            return create(path, ParquetSchemaMapper.nodeFileSchema(nodeType),
                    ParquetFooter.forNode(nodeType.labels(), nodeType.properties()),
                    nodeType.properties());
        }

        static EntityWriter forRelationship(Path path, RelationshipTypeSchema relType) {
            return create(path, ParquetSchemaMapper.relationshipFileSchema(relType),
                    ParquetFooter.forRelationship(relType.type(), relType.properties()),
                    relType.properties());
        }

        private static EntityWriter create(Path path, dev.hardwood.schema.FileSchema schema,
                                           ParquetFooter footer,
                                           SortedMap<String, PropertyType> types) {
            try {
                ParquetFileWriter file = ParquetFileWriter.create(OutputFile.of(path), schema, CONFIG);
                file.keyValueMetadata(footer.toKeyValueMetadata());
                return new EntityWriter(file, file.rowWriter(), types, path);
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create parquet file " + path, e);
            }
        }

        void writeRow(String context, java.util.function.Consumer<dev.hardwood.writer.StructBuilder> row) {
            try {
                rows.writeRow(row);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write " + context + " to " + path, e);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Parquet export failed: " + context + " has " + e.getMessage()
                                + " The export directory is incomplete.", e);
            }
        }

        void close() {
            try {
                file.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close parquet file " + path, e);
            }
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetDataWriterTest`
Expected: PASS, 9 tests. If the mismatch-context assertions fail, check that
`ParquetValues.set`'s message starts with `property '...'` — the writer concatenates
`"... has " + e.getMessage()` to form `node 4711 (:Person) has property 'name' is of type ...`.
If that reads badly, adjust `ParquetValues.mismatch` to return
`"property 'name' of type INTEGER, but the schema scan inferred STRING."` and the writer's
concatenation to match; keep entity, property, expected and actual in the final message.

- [ ] **Step 6: Run the full suite**

Run: `mvn -q test`
Expected: PASS, no regressions.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/parquet/ParquetDataWriter.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java \
        src/test/java/org/neo4j/dbcopy/parquet/TestEntities.java
git commit -m "feat: Add parquet data writer"
```

---

### Task 8: Bolt schema scanner

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/bolt/BoltSchemaScanner.java`
- Modify: `src/main/java/org/neo4j/dbcopy/CopyOptions.java`
- Modify: `src/main/java/org/neo4j/dbcopy/io/DataReader.java`
- Modify: `src/main/java/org/neo4j/dbcopy/bolt/BoltReader.java`
- Test: `src/test/java/org/neo4j/dbcopy/bolt/BoltSchemaScannerIT.java`
- Test: `src/test/java/org/neo4j/dbcopy/CopyOptionsTest.java`

**Interfaces:**
- Consumes: `GraphSchema`, `Neo4jTypeInference` (Tasks 2-3), existing `CopyOptions`, `DataReader`.
- Produces:
  - `int CopyOptions.schemaSampleSize()` (default `1000`) and `CopyOptions.Builder.schemaSampleSize(int)`
  - `default GraphSchema DataReader.scanSchema(CopyOptions copyOptions)` — throws by default
  - `public BoltSchemaScanner(Driver driver, String database)`
  - `GraphSchema BoltSchemaScanner.scan(CopyOptions copyOptions)`
  - `BoltReader.scanSchema` override

**Note on unlabelled nodes:** `db.schema.nodeTypeProperties()` only reports labelled nodes, so
the scanner probes the empty label set separately. Without that probe, a graph with unlabelled
nodes would fail at write time with "was not found in the scanned schema".

- [ ] **Step 1: Write the failing unit test for the sample size option**

Create `src/test/java/org/neo4j/dbcopy/CopyOptionsTest.java`:

```java
package org.neo4j.dbcopy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CopyOptionsTest {

    @Test
    void should_default_the_schema_sample_size_to_1000() {
        assertThat(CopyOptions.DEFAULT.schemaSampleSize()).isEqualTo(1000);
    }

    @Test
    void should_allow_overriding_the_schema_sample_size() {
        CopyOptions options = new CopyOptions.Builder().schemaSampleSize(50).build();

        assertThat(options.schemaSampleSize()).isEqualTo(50);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=CopyOptionsTest`
Expected: FAIL — `cannot find symbol: method schemaSampleSize()`.

- [ ] **Step 3: Add the option**

In `src/main/java/org/neo4j/dbcopy/CopyOptions.java`, add a field, accessor and builder method
following the existing `batchSize` pattern exactly:

```java
    private final int schemaSampleSize;
```

Assign it in the private constructor alongside `batchSize`:

```java
        this.schemaSampleSize = builder.schemaSampleSize;
```

Add the accessor after `batchSize()`:

```java
    /**
     * How many rows per node label set and per relationship type the parquet schema scan samples.
     * Not exposed as a CLI option yet.
     */
    public int schemaSampleSize() {
        return schemaSampleSize;
    }
```

And in `Builder`:

```java
        private int schemaSampleSize = 1000;

        public Builder schemaSampleSize(int schemaSampleSize) {
            this.schemaSampleSize = schemaSampleSize;
            return this;
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=CopyOptionsTest`
Expected: PASS, 2 tests.

- [ ] **Step 5: Write the failing integration test for the scanner**

Create `src/test/java/org/neo4j/dbcopy/bolt/BoltSchemaScannerIT.java`. It follows the container
setup in `src/test/java/org/neo4j/dbcopy/DataTransferIT.java:36-60` — copy that container
declaration verbatim, changing only the database name.

```java
package org.neo4j.dbcopy.bolt;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.INTEGER;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.POINT;
import static org.neo4j.dbcopy.schema.PropertyType.Scalar.STRING;

@SuppressWarnings("resource")
public class BoltSchemaScannerIT {

    public static final Logger LOG = LoggerFactory.getLogger(BoltSchemaScannerIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
    private static final String DB = "scandb";

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    static Driver driver;
    private Session session;

    @BeforeAll
    static void beforeAll() {
        neo4j.start();
        driver = GraphDatabase.driver(neo4j.getBoltUrl(), AUTH_TOKEN);
        driver.executableQuery("CREATE DATABASE " + DB + " IF NOT EXISTS WAIT").execute();
    }

    @AfterAll
    static void afterAll() {
        if (driver != null) {
            driver.close();
        }
    }

    @BeforeEach
    void setUp() {
        session = driver.session(SessionConfig.forDatabase(DB));
        session.run("MATCH (n) DETACH DELETE n").consume();
    }

    @Test
    void should_scan_label_sets_and_property_types() {
        session.run("""
                CREATE (:Person {name: 'Ada', age: 36, home: point({x: 2.35, y: 48.85})})
                CREATE (:Person:Employee {name: 'Bob', age: 40, empNo: 7})
                """).consume();

        GraphSchema schema = new BoltSchemaScanner(driver, DB).scan(CopyOptions.DEFAULT);

        assertThat(labelSets(schema)).containsExactlyInAnyOrder(
                Set.of("Person"), Set.of("Person", "Employee"));
        assertThat(propertiesOf(schema, Set.of("Person")))
                .containsEntry("name", STRING)
                .containsEntry("age", INTEGER)
                .containsEntry("home", POINT);
        assertThat(propertiesOf(schema, Set.of("Person", "Employee")))
                .containsEntry("empNo", INTEGER);
    }

    @Test
    void should_scan_unlabelled_nodes() {
        session.run("CREATE ({note: 'orphan'})").consume();

        GraphSchema schema = new BoltSchemaScanner(driver, DB).scan(CopyOptions.DEFAULT);

        assertThat(labelSets(schema)).contains(Set.of());
        assertThat(propertiesOf(schema, Set.of())).containsEntry("note", STRING);
    }

    @Test
    void should_scan_relationship_types_and_properties() {
        session.run("""
                CREATE (a:Person {name: 'Ada'})
                CREATE (b:Person {name: 'Bob'})
                CREATE (a)-[:KNOWS {since: 2020, tags: ['work']}]->(b)
                """).consume();

        GraphSchema schema = new BoltSchemaScanner(driver, DB).scan(CopyOptions.DEFAULT);

        assertThat(schema.relTypes()).hasSize(1);
        assertThat(schema.relTypes().get(0).type()).isEqualTo("KNOWS");
        assertThat(schema.relTypes().get(0).properties())
                .containsEntry("since", INTEGER)
                .containsEntry("tags", new PropertyType.ListOf(STRING));
    }

    @Test
    void should_skip_excluded_properties() {
        session.run("CREATE (:Person {name: 'Ada', secret: 'x'})").consume();
        CopyOptions options = new CopyOptions.Builder()
                .excludeNodeProperties(Set.of("secret"))
                .build();

        GraphSchema schema = new BoltSchemaScanner(driver, DB).scan(options);

        assertThat(propertiesOf(schema, Set.of("Person"))).containsOnlyKeys("name");
    }

    @Test
    void should_fail_on_inconsistent_property_types() {
        session.run("""
                CREATE (:Person {age: 36})
                CREATE (:Person {age: 'old'})
                """).consume();

        assertThatThrownBy(() -> new BoltSchemaScanner(driver, DB).scan(CopyOptions.DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("age")
                .hasMessageContaining("Person");
    }

    @Test
    void should_produce_an_empty_schema_for_an_empty_graph() {
        GraphSchema schema = new BoltSchemaScanner(driver, DB).scan(CopyOptions.DEFAULT);

        assertThat(schema.nodeTypes()).isEmpty();
        assertThat(schema.relTypes()).isEmpty();
    }

    private static List<Set<String>> labelSets(GraphSchema schema) {
        return schema.nodeTypes().stream().map(t -> (Set<String>) t.labels()).toList();
    }

    private static java.util.Map<String, PropertyType> propertiesOf(GraphSchema schema,
                                                                    Set<String> labels) {
        return schema.nodeTypes().stream()
                .filter(t -> t.labels().equals(labels))
                .findFirst()
                .map(NodeTypeSchema::properties)
                .orElseThrow(() -> new AssertionError("no node type for labels " + labels));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `mvn -q verify -Dit.test=BoltSchemaScannerIT -DskipUTs=true`
Expected: FAIL — `cannot find symbol: class BoltSchemaScanner`. If the property names above are
not wired in this project's Failsafe config, just run `mvn -q verify -Dit.test=BoltSchemaScannerIT`.
Docker must be running.

- [ ] **Step 7: Write the implementation**

Create `src/main/java/org/neo4j/dbcopy/bolt/BoltSchemaScanner.java`:

```java
package org.neo4j.dbcopy.bolt;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.MapAccessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.neo4j.driver.Values.parameters;

/**
 * Infers a {@link GraphSchema} by sampling a bounded number of rows per node label set and per
 * relationship type. Label sets and relationship types are discovered exhaustively; only the
 * property types are sampled, which is why a non-normalized graph can still surprise the writer.
 */
public final class BoltSchemaScanner {

    private static final String WARNING = """
            Parquet export is experimental. The graph schema is inferred by sampling %d nodes per \
            label set and %d relationships per type. The export will fail if property types are not \
            consistent (for example 'age' is an Integer on some :Person nodes and a String on \
            others), or if a property is absent from every sampled row.""";

    private final Driver driver;
    private final String database;

    public BoltSchemaScanner(Driver driver, String database) {
        this.driver = driver;
        this.database = database;
    }

    public GraphSchema scan(CopyOptions copyOptions) {
        int sampleSize = copyOptions.schemaSampleSize();
        System.err.println(WARNING.formatted(sampleSize, sampleSize));

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            List<NodeTypeSchema> nodeTypes = scanNodeTypes(session, sampleSize, copyOptions);
            List<RelationshipTypeSchema> relTypes = scanRelationshipTypes(session, sampleSize, copyOptions);
            return new GraphSchema(nodeTypes, relTypes);
        }
    }

    private List<NodeTypeSchema> scanNodeTypes(Session session, int sampleSize,
                                               CopyOptions copyOptions) {
        List<NodeTypeSchema> nodeTypes = new ArrayList<>();
        for (SortedSet<String> labels : discoverLabelSets(session)) {
            SortedMap<String, PropertyType> properties = sampleNodeProperties(
                    session, labels, sampleSize, copyOptions.getNodePropertiesToExclude());
            if (properties != null) {
                nodeTypes.add(new NodeTypeSchema(labels, properties));
            }
        }
        return nodeTypes;
    }

    /**
     * {@code db.schema.nodeTypeProperties()} reports only labelled nodes, so the empty label set is
     * probed separately below by {@link #sampleNodeProperties}.
     */
    private List<SortedSet<String>> discoverLabelSets(Session session) {
        List<SortedSet<String>> labelSets = new ArrayList<>();
        labelSets.add(new TreeSet<>());
        session.run("""
                        CALL db.schema.nodeTypeProperties() YIELD nodeLabels
                        WITH DISTINCT nodeLabels RETURN nodeLabels""")
                .list()
                .forEach(record -> labelSets.add(
                        new TreeSet<>(record.get("nodeLabels").asList(Value::asString))));
        return labelSets;
    }

    /**
     * @return the sampled property types, or {@code null} when the label set has no nodes at all.
     */
    private SortedMap<String, PropertyType> sampleNodeProperties(Session session,
                                                                 SortedSet<String> labels,
                                                                 int sampleSize,
                                                                 Set<String> excluded) {
        String query = "MATCH (n" + labelPredicate(labels) + ") WHERE size(labels(n)) = $labelCount "
                + "WITH n LIMIT $sample RETURN n";
        List<Record> records = session.run(query,
                parameters("labelCount", labels.size(), "sample", sampleSize)).list();
        if (records.isEmpty()) {
            return null;
        }
        return mergeProperties(records, "n", excluded, describe(labels));
    }

    private List<RelationshipTypeSchema> scanRelationshipTypes(Session session, int sampleSize,
                                                               CopyOptions copyOptions) {
        List<RelationshipTypeSchema> relTypes = new ArrayList<>();
        List<String> types = session
                .run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                .list(record -> record.get("relationshipType").asString());

        for (String type : types) {
            String query = "MATCH ()-[r:" + quote(type) + "]->() WITH r LIMIT $sample RETURN r";
            List<Record> records = session.run(query, parameters("sample", sampleSize)).list();
            if (records.isEmpty()) {
                continue;
            }
            relTypes.add(new RelationshipTypeSchema(type, mergeProperties(records, "r",
                    copyOptions.getRelationshipPropertiesToExclude(), "relationship type " + type)));
        }
        return relTypes;
    }

    private static SortedMap<String, PropertyType> mergeProperties(List<Record> records,
                                                                   String column,
                                                                   Set<String> excluded,
                                                                   String context) {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        for (Record record : records) {
            Entity entity = record.get(column).asEntity();
            for (String key : entity.keys()) {
                if (excluded.contains(key)) {
                    continue;
                }
                PropertyType observed = inferOrFail(entity, key, context);
                properties.merge(key, observed, (existing, seen) -> mergeOrFail(existing, seen, key, context));
            }
        }
        return properties;
    }

    private static PropertyType inferOrFail(MapAccessor entity, String key, String context) {
        try {
            return Neo4jTypeInference.inferType(entity.get(key));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Schema scan failed on " + context + ", property '"
                    + key + "': " + e.getMessage(), e);
        }
    }

    private static PropertyType mergeOrFail(PropertyType existing, PropertyType observed,
                                            String key, String context) {
        try {
            return Neo4jTypeInference.merge(existing, observed);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Schema scan failed on " + context + ": property '"
                    + key + "' does not have a consistent type. " + e.getMessage()
                    + " Parquet export requires one type per property.", e);
        }
    }

    private static String labelPredicate(SortedSet<String> labels) {
        StringBuilder predicate = new StringBuilder();
        labels.forEach(label -> predicate.append(':').append(quote(label)));
        return predicate.toString();
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String describe(Set<String> labels) {
        return labels.isEmpty() ? "unlabelled nodes" : "label set :" + String.join(":", labels);
    }
}
```

In `src/main/java/org/neo4j/dbcopy/io/DataReader.java`, add the imports and the new default
method next to `createStateManager`:

```java
    /**
     * Infers the source graph's schema. Required by targets that need a column schema up front,
     * such as parquet.
     */
    default GraphSchema scanSchema(CopyOptions copyOptions) {
        throw new IllegalArgumentException(
                "A parquet target requires a bolt/neo4j or parquet source.");
    }
```

In `src/main/java/org/neo4j/dbcopy/bolt/BoltReader.java`, add the override:

```java
    @Override
    public GraphSchema scanSchema(CopyOptions copyOptions) {
        return new BoltSchemaScanner(driver, databaseName).scan(copyOptions);
    }
```

- [ ] **Step 8: Run the integration test to verify it passes**

Run: `mvn -q verify -Dit.test=BoltSchemaScannerIT`
Expected: PASS, 6 tests.

- [ ] **Step 9: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/bolt/BoltSchemaScanner.java \
        src/main/java/org/neo4j/dbcopy/bolt/BoltReader.java \
        src/main/java/org/neo4j/dbcopy/io/DataReader.java \
        src/main/java/org/neo4j/dbcopy/CopyOptions.java \
        src/test/java/org/neo4j/dbcopy/bolt/BoltSchemaScannerIT.java \
        src/test/java/org/neo4j/dbcopy/CopyOptionsTest.java
git commit -m "feat: Scan the source graph schema over bolt"
```

---

### Task 9: Wire the parquet target into the CLI

**Files:**
- Modify: `src/main/java/org/neo4j/dbcopy/io/EndpointFactorySupport.java`
- Modify: `src/main/java/org/neo4j/dbcopy/io/DataWriterFactory.java`
- Modify: `src/main/java/org/neo4j/dbcopy/io/DataReaderFactory.java`
- Modify: `src/main/java/org/neo4j/dbcopy/DbCopyCli.java`
- Modify: `README.adoc`
- Test: `src/test/java/org/neo4j/dbcopy/io/EndpointFactorySupportTest.java`
- Test: `src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java`

**Interfaces:**
- Consumes: `ParquetDataWriter` (Task 7), `DataReader.scanSchema` (Task 8).
- Produces:
  - `EndpointFactorySupport.EndpointKind.PARQUET`
  - `protected Path EndpointFactorySupport.toParquetDirectoryPath(URI uri, String endpointName)`
  - `public boolean DataWriterFactory.needsSchema(URI targetAddress)`
  - `createWriter(..., GraphSchema schema)` — new trailing parameter

After this task the export path works end to end from the command line.

- [ ] **Step 1: Write the failing tests**

Add to `src/test/java/org/neo4j/dbcopy/io/EndpointFactorySupportTest.java`, inside the class, and
add a matching accessor to the nested `TestEndpointFactorySupport` (it currently exposes
`endpointKindFor`, `requireOptionValue` and `jsonFilePath`):

```java
    @Test
    void should_detect_parquet_scheme() {
        assertThat(support.endpointKindFor(URI.create("parquet:///tmp/export")))
                .isEqualTo(EndpointFactorySupport.EndpointKind.PARQUET);
    }

    @Test
    void should_convert_parquet_uri_to_directory_path() {
        assertThat(support.parquetDirectoryPath(URI.create("parquet:///tmp/export"), "target"))
                .isEqualTo(Path.of("/tmp/export"));
    }

    @Test
    void should_reject_a_parquet_uri_without_a_path() {
        assertThatThrownBy(() -> support.parquetDirectoryPath(URI.create("parquet://"), "target"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must point to a directory");
    }
```

```java
        Path parquetDirectoryPath(URI uri, String endpointName) {
            return toParquetDirectoryPath(uri, endpointName);
        }
```

Add to `src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java`:

```java
    @Test
    void should_reject_a_parquet_target_with_a_json_source() throws Exception {
        Path sourceFile = Files.createTempFile("dbcopy-source", ".jsonl");
        Path targetDir = Files.createTempDirectory("dbcopy-parquet-target");

        CommandResult result = execute(
                "-sa", sourceFile.toUri().toString(),
                "-ta", "parquet://" + targetDir.resolve("export")
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("A parquet target requires a bolt/neo4j or parquet source.");
    }

    @Test
    void should_reject_a_parquet_source_for_now() throws Exception {
        Path targetFile = Files.createTempFile("dbcopy-target", ".jsonl");

        CommandResult result = execute(
                "-sa", "parquet:///tmp/does-not-matter",
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("Parquet is supported as a target only");
    }
```

The second test is deleted in Task 13, when parquet becomes a valid source.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q test -Dtest='EndpointFactorySupportTest+DbCopyCliTest'`
Expected: FAIL — `cannot find symbol: PARQUET`.

- [ ] **Step 3: Add the endpoint kind and path helper**

In `src/main/java/org/neo4j/dbcopy/io/EndpointFactorySupport.java`, add `PARQUET` to the enum:

```java
    protected enum EndpointKind {
        BOLT,
        JSON,
        PARQUET
    }
```

In `endpointKind(URI)`, add the new scheme before the final throw, and extend the error message:

```java
        if ("parquet".equals(lower)) {
            return EndpointKind.PARQUET;
        }
        throw new IllegalArgumentException("Unsupported scheme '" + scheme + "' in address: " + uri +
                ". Supported schemes are bolt/neo4j, file and parquet.");
    }
```

Add the directory helper:

```java
    protected Path toParquetDirectoryPath(URI uri, String endpointName) {
        if (!"parquet".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("The " + endpointName +
                    " parquet endpoint must use a parquet URI: " + uri);
        }
        String path = uri.getPath();
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new IllegalArgumentException("The " + endpointName +
                    " parquet endpoint must point to a directory: " + uri);
        }
        return Path.of(path);
    }
```

- [ ] **Step 4: Wire the writer factory**

In `src/main/java/org/neo4j/dbcopy/io/DataWriterFactory.java`, add the schema parameter and the
new switch branch:

```java
    public boolean needsSchema(URI targetAddress) {
        return endpointKind(targetAddress) == EndpointKind.PARQUET;
    }

    public DataWriter createWriter(URI targetAddress, String targetUserName, String targetPassword,
                                   String targetDatabase, boolean createTargetDatabase,
                                   GraphSchema schema) {
        EndpointKind kind = endpointKind(targetAddress);
        return switch (kind) {
            case BOLT ->
                    createBoltWriter(targetAddress, targetUserName, targetPassword, targetDatabase, createTargetDatabase);
            case JSON -> new JsonDataWriter(toJsonFilePath(targetAddress, "target"));
            case PARQUET -> new ParquetDataWriter(
                    toParquetDirectoryPath(targetAddress, "target"), schema);
        };
    }
```

Add the imports for `ParquetDataWriter` and `GraphSchema`.

In `src/main/java/org/neo4j/dbcopy/io/DataReaderFactory.java`, reject a parquet source for now:

```java
            case PARQUET -> throw new IllegalArgumentException(
                    "Parquet is supported as a target only.");
```

- [ ] **Step 5: Wire the CLI**

In `src/main/java/org/neo4j/dbcopy/DbCopyCli.java`, inside `call()`, replace the reader/writer
setup so the schema is scanned between them:

```java
            reader = dataReaderFactory.createReader(sourceAddress, sourceUserName, sourcePassword, sourceDatabase);
            databaseStateManager = reader.createStateManager(lockSourceDatabase);
            databaseStateManager.makeReadOnly();
            GraphSchema schema = dataWriterFactory.needsSchema(targetAddress)
                    ? reader.scanSchema(copyOptions)
                    : null;
            writer = dataWriterFactory.createWriter(targetAddress, targetUserName, targetPassword,
                    targetDatabase, createTargetDatabase, schema);
```

Add the `org.neo4j.dbcopy.schema.GraphSchema` import. Also update the `@Command` description and
the two address option descriptions to mention parquet:

```java
        description = """
                Copy the content of a Neo4j database to another Neo4j database, a JSONL file, or a parquet directory.
                If the source/target address starts with bolt/neo4j, Bolt is used.
                If it starts with file, a JSONL file is used.
                If it starts with parquet, a directory of parquet files is used."""
```

and, in both `-sa` and `-ta` descriptions, add the line:

```java
                    "or parquet:///path/to/dir for parquet"
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -q test -Dtest='EndpointFactorySupportTest+DbCopyCliTest'`
Expected: PASS.

- [ ] **Step 7: Run the full unit suite**

Run: `mvn -q test`
Expected: PASS, no regressions. `DataTransferTest` and `EndpointFactoryIT` construct
`createWriter(...)`; update those call sites with a trailing `null` schema argument if the
compiler flags them.

- [ ] **Step 8: Document it**

In `README.adoc`, make three edits.

Replace the intro sentence:

```adoc
A development tool useful to copy the data from a Neo4j database to another Neo4j database, to and from a JSONL file, or to a directory of parquet files.
```

After the paragraph describing the JSONL format, add:

```adoc
The parquet export writes a directory: one file per distinct node label set under `nodes/`, and
one file per relationship type under `relationships/`. Identity columns are `__dbcopy_id`,
`__dbcopy_labels` for nodes and `__dbcopy_id`, `__dbcopy_source_id`, `__dbcopy_target_id`,
`__dbcopy_type` for relationships. Each file records the Neo4j type of every property in its
parquet footer, so no type information is lost.

Parquet export is experimental: the graph schema is inferred by sampling 1000 nodes per label set
and 1000 relationships per type. If a property does not have one consistent type across a label
set, the export fails rather than writing incorrect data.
```

Add an example to the Examples block:

```adoc
# Neo4j -> parquet
neo4j-db-copy \
  -sa neo4j+s://source.example:7687 -su neo4j -sp -sd sourcedb \
  -ta parquet:///tmp/export
```

- [ ] **Step 9: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/io src/main/java/org/neo4j/dbcopy/DbCopyCli.java \
        README.adoc src/test/java/org/neo4j/dbcopy/io/EndpointFactorySupportTest.java \
        src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java
git commit -m "feat: Add parquet target endpoint to the CLI"
```

---

### Task 10: Export integration test

**Files:**
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetExportIT.java`

**Interfaces:**
- Consumes: everything from Tasks 1-9. Produces nothing new.

This is the first test that exercises a real graph containing every type in the mapping table.
No production code should be needed — if something fails here, it is a bug in Tasks 6-8.

- [ ] **Step 1: Write the test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetExportIT.java`. Copy the container
declaration from `src/test/java/org/neo4j/dbcopy/DataTransferIT.java:36-60`, changing the
database name to `exportdb`.

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.bolt.BoltReader;
import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("resource")
public class ParquetExportIT {

    public static final Logger LOG = LoggerFactory.getLogger(ParquetExportIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
    private static final String DB = "exportdb";

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    static Driver driver;
    private Session session;

    @BeforeAll
    static void beforeAll() {
        neo4j.start();
        driver = GraphDatabase.driver(neo4j.getBoltUrl(), AUTH_TOKEN);
        driver.executableQuery("CREATE DATABASE " + DB + " IF NOT EXISTS WAIT").execute();
    }

    @AfterAll
    static void afterAll() {
        if (driver != null) {
            driver.close();
        }
    }

    @BeforeEach
    void setUp() {
        session = driver.session(SessionConfig.forDatabase(DB));
        session.run("MATCH (n) DETACH DELETE n").consume();
    }

    @Test
    void should_export_every_supported_type(@TempDir Path dir) throws Exception {
        session.run("""
                CREATE (a:Person {
                    name: 'Ada',
                    age: 36,
                    ratio: 2.5,
                    active: true,
                    born: date('1815-12-10'),
                    wakeup: localtime('07:30:00.000000042'),
                    alarm: time('07:30:00+02:00'),
                    seen: localdatetime('2024-01-15T10:30:00.000000042'),
                    createdZoned: datetime('2024-01-15T10:30:00[Europe/Paris]'),
                    createdOffset: datetime('2024-01-15T10:30:00+02:00'),
                    elapsed: duration({months: 1, days: 2, seconds: 3, nanoseconds: 4}),
                    here: point({x: 2.35, y: 48.85}),
                    there: point({x: 1.0, y: 2.0, z: 3.0}),
                    tags: ['a', 'b'],
                    scores: [1, 2, 3],
                    empties: []
                })
                CREATE (b:Person:Employee {name: 'Bob', empNo: 7})
                CREATE (c {note: 'orphan'})
                CREATE (a)-[:KNOWS {since: 2020, weights: [1.5, 2.5]}]->(b)
                CREATE (a)-[:WROTE]->(b)
                """).consume();

        Path export = dir.resolve("export");
        copyToParquet(export);

        assertThat(export.resolve("nodes/Person.parquet")).exists();
        assertThat(export.resolve("nodes/Employee__Person.parquet")).exists();
        assertThat(export.resolve("nodes/_no_labels.parquet")).exists();
        assertThat(export.resolve("relationships/KNOWS.parquet")).exists();
        assertThat(export.resolve("relationships/WROTE.parquet")).exists();

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(export.resolve("nodes/Person.parquet")));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getString("name")).isEqualTo("Ada");
            assertThat(rows.getLong("age")).isEqualTo(36L);
            assertThat(rows.getDate("born")).isEqualTo(java.time.LocalDate.of(1815, 12, 10));
            assertThat(rows.getTime("wakeup").getNano()).isEqualTo(42);
            assertThat(rows.getStruct("alarm").getInt("offsetSeconds")).isEqualTo(7200);
            assertThat(rows.getStruct("seen").getInt("nano")).isEqualTo(42);
            assertThat(rows.getStruct("createdZoned").getString("zoneId")).isEqualTo("Europe/Paris");
            assertThat(rows.getStruct("createdOffset").isNull("zoneId")).isTrue();
            assertThat(rows.getStruct("elapsed").getLong("months")).isEqualTo(1L);
            assertThat(rows.getStruct("here").isNull("z")).isTrue();
            assertThat(rows.getStruct("there").getDouble("z")).isEqualTo(3.0);
            assertThat(rows.getList("tags").strings()).containsExactly("a", "b");
            assertThat(rows.getList("scores").longs().size()).isEqualTo(3);
            assertThat(rows.getList("empties").isEmpty()).isTrue();
        }
    }

    @Test
    void should_refuse_a_non_empty_target_directory(@TempDir Path dir) throws Exception {
        session.run("CREATE (:Person {name: 'Ada'})").consume();
        Path export = dir.resolve("export");
        Files.createDirectories(export);
        Files.writeString(export.resolve("stale.txt"), "x");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> copyToParquet(export))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Refusing to write into non-empty parquet export directory");
    }

    @Test
    void should_fail_on_a_non_normalized_property_type(@TempDir Path dir) {
        session.run("""
                CREATE (:Person {age: 36})
                CREATE (:Person {age: 'old'})
                """).consume();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> copyToParquet(dir.resolve("export")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("age");
    }

    private void copyToParquet(Path export) throws Exception {
        DataReader reader = new NonClosingBoltReader(driver, DB);
        GraphSchema schema = reader.scanSchema(CopyOptions.DEFAULT);
        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema)) {
            writer.writeNodes(reader.readNodes(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(reader.readRelationships(), CopyOptions.DEFAULT).blockLast();
        }
    }

    /** {@code BoltReader.close()} closes the shared driver, which the other tests still need. */
    private static final class NonClosingBoltReader extends BoltReader {
        NonClosingBoltReader(Driver driver, String database) {
            super(driver, database);
        }

        @Override
        public void close() {
        }
    }
}
```

`BoltReader.close()` closes the driver it was given (`BoltReader.java:83`), and the driver here is
shared across all test methods by `@BeforeAll`. Hence the non-closing subclass — closing it in the
first test would break the two that follow.

- [ ] **Step 2: Run the integration test**

Run: `mvn -q verify -Dit.test=ParquetExportIT`
Expected: PASS, 3 tests. Docker must be running.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/org/neo4j/dbcopy/parquet/ParquetExportIT.java
git commit -m "test: Add parquet export integration test covering every property type"
```

**The export path is complete and shippable at this point.** Everything below adds import.

---

### Task 11: Extract shared Node and Relationship records

**Files:**
- Create: `src/main/java/org/neo4j/dbcopy/io/NodeRecord.java`
- Create: `src/main/java/org/neo4j/dbcopy/io/RelationshipRecord.java`
- Modify: `src/main/java/org/neo4j/dbcopy/json/JsonDataReader.java`
- Delete: `src/test/java/org/neo4j/dbcopy/parquet/TestEntities.java`
- Modify: `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java`

**Interfaces:**
- Produces:
  - `public NodeRecord(long id, List<String> labels, Map<String, Value> properties) implements Node`
  - `public RelationshipRecord(long id, long startId, long endId, String type, Map<String, Value> properties) implements Relationship`

This is a pure refactor: no behaviour changes, and the existing tests are the safety net.
`JsonDataReader` currently carries ~200 lines of `Node`/`Relationship` boilerplate that
`ParquetDataReader` would otherwise have to duplicate.

- [ ] **Step 1: Move the classes**

Create `src/main/java/org/neo4j/dbcopy/io/NodeRecord.java` in package `org.neo4j.dbcopy.io`,
containing the body of `JsonDataReader`'s private `JsonNodeEntity` class (at the bottom of
`src/main/java/org/neo4j/dbcopy/json/JsonDataReader.java`) with these changes only:
- rename the class to `NodeRecord` and make it `public final`
- make the constructor `public`

Create `src/main/java/org/neo4j/dbcopy/io/RelationshipRecord.java` the same way from
`JsonRelationshipEntity`: rename to `RelationshipRecord`, make it a `public record` with the same
component list `(long id, long startId, long endId, String type, Map<String, Value> properties)`,
keeping the existing compact constructor that calls `Map.copyOf(properties)`.

- [ ] **Step 2: Point JsonDataReader at them**

In `src/main/java/org/neo4j/dbcopy/json/JsonDataReader.java`:
- delete the `JsonNodeEntity` and `JsonRelationshipEntity` nested classes
- change `toNode` to `return new NodeRecord(id, labels, properties);`
- change `toRelationship` to `return new RelationshipRecord(id, startId, endId, type, properties);`
- add `import org.neo4j.dbcopy.io.NodeRecord;` and `import org.neo4j.dbcopy.io.RelationshipRecord;`
- remove now-unused imports (`java.util.function.Function` and `org.neo4j.driver.types.Node`/
  `Relationship` may still be needed by the `Flux<Node>` signatures — let the compiler tell you)

- [ ] **Step 3: Run the JSON tests to prove nothing changed**

Run: `mvn -q test -Dtest=JsonDataReaderWriterTest`
Expected: PASS, unchanged.

- [ ] **Step 4: Repoint the parquet writer test and delete the stopgap**

In `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java`, replace every
`TestEntities.node(...)` with `new NodeRecord(...)` and every `TestEntities.relationship(...)`
with `new RelationshipRecord(...)`, adding the two imports. Then delete
`src/test/java/org/neo4j/dbcopy/parquet/TestEntities.java`.

- [ ] **Step 5: Run the full unit suite**

Run: `mvn -q test`
Expected: PASS, no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/io/NodeRecord.java \
        src/main/java/org/neo4j/dbcopy/io/RelationshipRecord.java \
        src/main/java/org/neo4j/dbcopy/json/JsonDataReader.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetDataWriterTest.java
git rm src/test/java/org/neo4j/dbcopy/parquet/TestEntities.java
git commit -m "refactor: Share Node and Relationship records between file readers"
```

---

### Task 12: Parquet data reader

**Files:**
- Modify: `src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java`
- Create: `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataReader.java`
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataReaderTest.java`

**Interfaces:**
- Consumes: `ParquetValues.set` (Task 6), `ParquetFooter` (Task 5), `ParquetLayout` (Task 3), `NodeRecord`/`RelationshipRecord` (Task 11), `ParquetDataWriter` (Task 7, used by the test to build fixtures).
- Produces:
  - `static Value ParquetValues.read(StructAccessor row, String field, PropertyType type)` — returns `null` when the field is null, so callers omit the property entirely
  - `public ParquetDataReader(Path directory)` implementing `DataReader`

**Why counts are cheap:** `FileMetaData.numRows()` comes from the footer, so
`getTotalNodeCount()` reads metadata only. The JSONL reader has to parse every line for the same
answer (`JsonDataReader.java:84`).

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetDataReaderTest.java`:

```java
package org.neo4j.dbcopy.parquet;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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

class ParquetDataReaderTest {

    @Test
    void should_round_trip_every_supported_type(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("active", BOOLEAN);
        types.put("age", INTEGER);
        types.put("alarm", OFFSET_TIME);
        types.put("born", DATE);
        types.put("createdOffset", DATE_TIME);
        types.put("createdZoned", DATE_TIME);
        types.put("elapsed", DURATION);
        types.put("empties", new PropertyType.ListOf(UNKNOWN));
        types.put("here", POINT);
        types.put("name", STRING);
        types.put("payload", BYTE_ARRAY);
        types.put("ratio", FLOAT);
        types.put("scores", new PropertyType.ListOf(INTEGER));
        types.put("seen", LOCAL_DATE_TIME);
        types.put("tags", new PropertyType.ListOf(STRING));
        types.put("there", POINT);
        types.put("wakeup", LOCAL_TIME);

        ZonedDateTime zoned = ZonedDateTime.parse("2024-01-15T10:30:00.000000042+01:00[Europe/Paris]");
        OffsetDateTime offset = OffsetDateTime.parse("2024-01-15T10:30:00Z");
        Map<String, Value> properties = Map.ofEntries(
                Map.entry("active", Values.value(true)),
                Map.entry("age", Values.value(36L)),
                Map.entry("alarm", Values.value(OffsetTime.of(LocalTime.of(7, 30), ZoneOffset.ofHours(2)))),
                Map.entry("born", Values.value(LocalDate.of(1815, 12, 10))),
                Map.entry("createdOffset", Values.value(offset)),
                Map.entry("createdZoned", Values.value(zoned)),
                Map.entry("elapsed", Values.isoDuration(1, 2, 3, 4)),
                Map.entry("empties", Values.value(List.of())),
                Map.entry("here", Values.point(4326, 2.35, 48.85)),
                Map.entry("name", Values.value("Ada")),
                Map.entry("payload", Values.value(new byte[]{1, 2, 3})),
                Map.entry("ratio", Values.value(2.5d)),
                Map.entry("scores", Values.value(List.of(1L, 2L, 3L))),
                Map.entry("seen", Values.value(LocalDateTime.of(2024, 1, 15, 10, 30, 0, 42))),
                Map.entry("tags", Values.value(List.of("a", "b"))),
                Map.entry("there", Values.point(4979, 1.0, 2.0, 3.0)),
                Map.entry("wakeup", Values.value(LocalTime.of(7, 30, 0, 42))));

        Path export = write(dir, types, new NodeRecord(1L, List.of("Person"), properties), null);

        List<Node> nodes;
        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            nodes = reader.readNodes().collectList().block();
        }

        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.id()).isEqualTo(1L);
        assertThat(node.labels()).containsExactly("Person");
        Map<String, Object> read = node.asMap();
        assertThat(read).containsEntry("active", true)
                .containsEntry("age", 36L)
                .containsEntry("ratio", 2.5d)
                .containsEntry("name", "Ada")
                .containsEntry("born", LocalDate.of(1815, 12, 10))
                .containsEntry("wakeup", LocalTime.of(7, 30, 0, 42))
                .containsEntry("alarm", OffsetTime.of(LocalTime.of(7, 30), ZoneOffset.ofHours(2)))
                .containsEntry("seen", LocalDateTime.of(2024, 1, 15, 10, 30, 0, 42))
                .containsEntry("createdZoned", zoned)
                .containsEntry("createdOffset", offset)
                .containsEntry("tags", List.of("a", "b"))
                .containsEntry("scores", List.of(1L, 2L, 3L))
                .containsEntry("empties", List.of());
        assertThat((byte[]) read.get("payload")).containsExactly(1, 2, 3);
        assertThat(node.get("elapsed").asIsoDuration().months()).isEqualTo(1L);
        assertThat(node.get("elapsed").asIsoDuration().nanoseconds()).isEqualTo(4);
        assertThat(node.get("here").asPoint().srid()).isEqualTo(4326);
        assertThat(node.get("here").asPoint().z()).isNaN();
        assertThat(node.get("there").asPoint().z()).isEqualTo(3.0);
    }

    @Test
    void should_omit_a_property_that_is_null_in_the_file(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("age", INTEGER);
        types.put("name", STRING);

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))), null);

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            Node node = reader.readNodes().blockFirst();
            assertThat(node.keys()).containsExactly("name");
            assertThat(node.containsKey("age")).isFalse();
        }
    }

    @Test
    void should_read_relationships(@TempDir Path dir) throws Exception {
        Path export = write(dir, new TreeMap<>(), null,
                new RelationshipRecord(7L, 1L, 2L, "KNOWS", Map.of("since", Values.value(2020L))));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            Relationship relationship = reader.readRelationships().blockFirst();
            assertThat(relationship.id()).isEqualTo(7L);
            assertThat(relationship.startNodeId()).isEqualTo(1L);
            assertThat(relationship.endNodeId()).isEqualTo(2L);
            assertThat(relationship.type()).isEqualTo("KNOWS");
            assertThat(relationship.asMap()).containsEntry("since", 2020L);
        }
    }

    @Test
    void should_count_rows_from_the_footers(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("name", STRING);

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))),
                new RelationshipRecord(7L, 1L, 1L, "KNOWS", Map.of()));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            assertThat(reader.getTotalNodeCount()).isEqualTo(1L);
            assertThat(reader.getTotalRelationshipCount()).isEqualTo(1L);
        }
    }

    @Test
    void should_rebuild_the_schema_from_the_footers(@TempDir Path dir) throws Exception {
        SortedMap<String, PropertyType> types = new TreeMap<>();
        types.put("name", STRING);
        types.put("tags", new PropertyType.ListOf(STRING));

        Path export = write(dir, types,
                new NodeRecord(1L, List.of("Person"), Map.of("name", Values.value("Ada"))), null);

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            GraphSchema schema = reader.scanSchema(CopyOptions.DEFAULT);
            assertThat(schema.nodeTypes()).hasSize(1);
            assertThat(schema.nodeTypes().get(0).labels()).containsExactly("Person");
            assertThat(schema.nodeTypes().get(0).properties())
                    .containsEntry("name", STRING)
                    .containsEntry("tags", new PropertyType.ListOf(STRING));
        }
    }

    @Test
    void should_reject_a_directory_that_is_not_an_export(@TempDir Path dir) {
        assertThatThrownBy(() -> new ParquetDataReader(dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a neo4j-db-copy parquet export");
    }

    @Test
    void should_reject_a_parquet_file_without_a_dbcopy_footer(@TempDir Path dir) throws Exception {
        Path export = dir.resolve("export");
        Files.createDirectories(export.resolve("nodes"));
        Files.copy(foreignParquetFile(dir), export.resolve("nodes/foreign.parquet"));

        try (ParquetDataReader reader = new ParquetDataReader(export)) {
            assertThatThrownBy(() -> reader.readNodes().collectList().block())
                    .hasMessageContaining("is not a neo4j-db-copy parquet export");
        }
    }

    /** A valid parquet file with no {@code neo4j.*} footer keys. */
    private static Path foreignParquetFile(Path dir) throws Exception {
        Path file = dir.resolve("foreign-source.parquet");
        dev.hardwood.schema.FileSchema schema = dev.hardwood.schema.FileSchema.builder("foreign")
                .addColumn("x", dev.hardwood.metadata.PhysicalType.INT64,
                        dev.hardwood.metadata.RepetitionType.REQUIRED)
                .build();
        try (dev.hardwood.writer.ParquetFileWriter writer = dev.hardwood.writer.ParquetFileWriter
                .create(dev.hardwood.OutputFile.of(file), schema)) {
            writer.rowWriter().writeRow(row -> row.setLong("x", 1L));
        }
        return file;
    }

    private static Path write(Path dir,
                              SortedMap<String, PropertyType> nodeProperties,
                              NodeRecord node,
                              RelationshipRecord relationship) throws Exception {
        Path export = dir.resolve("export-" + System.nanoTime());
        SortedMap<String, PropertyType> relProperties = new TreeMap<>();
        if (relationship != null) {
            relationship.properties().forEach((name, value) ->
                    relProperties.put(name, org.neo4j.dbcopy.schema.Neo4jTypeInference.inferType(value)));
        }
        GraphSchema schema = new GraphSchema(
                List.of(new NodeTypeSchema(new TreeSet<>(List.of("Person")), nodeProperties)),
                List.of(new RelationshipTypeSchema("KNOWS", relProperties)));

        try (ParquetDataWriter writer = new ParquetDataWriter(export, schema)) {
            writer.writeNodes(node == null ? Flux.empty() : Flux.just(node), CopyOptions.DEFAULT)
                    .blockLast();
            writer.writeRelationships(
                    relationship == null ? Flux.empty() : Flux.just(relationship), CopyOptions.DEFAULT)
                    .blockLast();
        }
        return export;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ParquetDataReaderTest`
Expected: FAIL — `cannot find symbol: class ParquetDataReader`.

- [ ] **Step 3: Add the read side to ParquetValues**

Append to `src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java` (and add imports for
`dev.hardwood.row.PqList`, `dev.hardwood.row.PqStruct`, `dev.hardwood.row.StructAccessor`,
`java.time.Instant`, `java.time.ZoneId`, `java.util.ArrayList`):

```java
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
```

Also relax `ParquetValues`' class visibility if needed: it is package-private, which is fine
because `ParquetDataReader` lives in the same package.

`PqList.get(int)` returns the logical-typed Java object for primitive elements — a `String`,
`Long`, `LocalDate` and so on. If it turns out to return raw parquet values instead, switch the
`default` branch of `readElement` to the typed bulk accessors (`list.strings()`, `list.longs()`,
`list.dates()`, …) selected by element type, and read by index from those.

- [ ] **Step 4: Write the reader**

Create `src/main/java/org/neo4j/dbcopy/parquet/ParquetDataReader.java`:

```java
package org.neo4j.dbcopy.parquet;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.io.NodeRecord;
import org.neo4j.dbcopy.io.RelationshipRecord;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Reads a directory written by {@link ParquetDataWriter}. Property types come from each file's
 * footer, so nothing is inferred from the physical parquet layout.
 */
public final class ParquetDataReader implements DataReader {

    private final Path directory;

    public ParquetDataReader(Path directory) {
        ParquetLayout.requireImportDirectory(directory);
        this.directory = directory;
        System.err.println("Reading parquet export from " + directory);
    }

    @Override
    public Flux<Node> readNodes() {
        return Flux.fromIterable(files(ParquetLayout.NODES_DIR))
                .concatMap(file -> readFile(file, ParquetDataReader::toNode));
    }

    @Override
    public Flux<Relationship> readRelationships() {
        return Flux.fromIterable(files(ParquetLayout.RELATIONSHIPS_DIR))
                .concatMap(file -> readFile(file, ParquetDataReader::toRelationship));
    }

    @Override
    public long getTotalNodeCount() {
        return countRows(ParquetLayout.NODES_DIR);
    }

    @Override
    public long getTotalRelationshipCount() {
        return countRows(ParquetLayout.RELATIONSHIPS_DIR);
    }

    @Override
    public GraphSchema scanSchema(CopyOptions copyOptions) {
        List<NodeTypeSchema> nodeTypes = new ArrayList<>();
        for (Path file : files(ParquetLayout.NODES_DIR)) {
            ParquetFooter footer = readFooter(file);
            nodeTypes.add(new NodeTypeSchema(new TreeSet<>(footer.labels()),
                    filter(footer.types(), copyOptions.getNodePropertiesToExclude())));
        }
        List<RelationshipTypeSchema> relTypes = new ArrayList<>();
        for (Path file : files(ParquetLayout.RELATIONSHIPS_DIR)) {
            ParquetFooter footer = readFooter(file);
            relTypes.add(new RelationshipTypeSchema(footer.relationshipType(),
                    filter(footer.types(), copyOptions.getRelationshipPropertiesToExclude())));
        }
        return new GraphSchema(nodeTypes, relTypes);
    }

    private static SortedMap<String, PropertyType> filter(SortedMap<String, PropertyType> types,
                                                          Set<String> excluded) {
        SortedMap<String, PropertyType> kept = new TreeMap<>();
        types.forEach((name, type) -> {
            if (!excluded.contains(name)) {
                kept.put(name, type);
            }
        });
        return kept;
    }

    private List<Path> files(String subDirectory) {
        Path dir = directory.resolve(subDirectory);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to list parquet files in " + dir, e);
        }
    }

    private long countRows(String subDirectory) {
        long total = 0L;
        for (Path file : files(subDirectory)) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
                ParquetFooter.fromKeyValueMetadata(reader.getFileMetaData().keyValueMetadata(), file);
                total += reader.getFileMetaData().numRows();
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to read parquet file " + file, e);
            }
        }
        return total;
    }

    private ParquetFooter readFooter(Path file) {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return ParquetFooter.fromKeyValueMetadata(reader.getFileMetaData().keyValueMetadata(), file);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read parquet file " + file, e);
        }
    }

    private <T> Flux<T> readFile(Path file, java.util.function.BiFunction<RowReader, ParquetFooter, T> mapper) {
        return Flux.using(
                        () -> OpenFile.open(file),
                        open -> Flux.<T>generate(sink -> {
                            if (open.rows().hasNext()) {
                                open.rows().next();
                                sink.next(mapper.apply(open.rows(), open.footer()));
                            } else {
                                sink.complete();
                            }
                        }),
                        OpenFile::close)
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static Node toNode(RowReader row, ParquetFooter footer) {
        return new NodeRecord(row.getLong(ParquetLayout.ID),
                row.getList(ParquetLayout.LABELS).strings(),
                properties(row, footer));
    }

    private static Relationship toRelationship(RowReader row, ParquetFooter footer) {
        return new RelationshipRecord(row.getLong(ParquetLayout.ID),
                row.getLong(ParquetLayout.SOURCE_ID),
                row.getLong(ParquetLayout.TARGET_ID),
                row.getString(ParquetLayout.TYPE),
                properties(row, footer));
    }

    private static Map<String, Value> properties(RowReader row, ParquetFooter footer) {
        Map<String, Value> properties = new LinkedHashMap<>();
        footer.types().forEach((name, type) -> {
            Value value = ParquetValues.read(row, name, type);
            if (value != null) {
                properties.put(name, value);
            }
        });
        return properties;
    }

    private record OpenFile(ParquetFileReader reader, RowReader rows, ParquetFooter footer) {

        static OpenFile open(Path file) {
            ParquetFileReader reader = null;
            try {
                reader = ParquetFileReader.open(InputFile.of(file));
                ParquetFooter footer = ParquetFooter.fromKeyValueMetadata(
                        reader.getFileMetaData().keyValueMetadata(), file);
                return new OpenFile(reader, reader.rowReader(), footer);
            } catch (IOException e) {
                closeQuietly(reader);
                throw new UncheckedIOException("Unable to read parquet file " + file, e);
            } catch (RuntimeException e) {
                closeQuietly(reader);
                throw e;
            }
        }

        void close() {
            rows.close();
            closeQuietly(reader);
        }

        private static void closeQuietly(ParquetFileReader reader) {
            if (reader == null) {
                return;
            }
            try {
                reader.close();
            } catch (IOException ignored) {
            }
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=ParquetDataReaderTest`
Expected: PASS, 7 tests.

The round-trip test is the one to watch. If `createdZoned` comes back unequal, compare the
`Instant` and the zone separately — `ZonedDateTime.equals` also compares the stored offset, and a
zone-id round trip through `Instant` must reproduce it. If `wakeup` loses its 42 nanoseconds, the
`TimeType` unit is not `NANOS` somewhere in Task 4.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/parquet/ParquetValues.java \
        src/main/java/org/neo4j/dbcopy/parquet/ParquetDataReader.java \
        src/test/java/org/neo4j/dbcopy/parquet/ParquetDataReaderTest.java
git commit -m "feat: Add parquet data reader"
```

---

### Task 13: Wire the parquet source into the CLI

**Files:**
- Modify: `src/main/java/org/neo4j/dbcopy/io/DataReaderFactory.java`
- Modify: `src/main/java/org/neo4j/dbcopy/DbCopyCli.java`
- Modify: `README.adoc`
- Test: `src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java`

**Interfaces:**
- Consumes: `ParquetDataReader` (Task 12).
- Produces: `parquet:` accepted as `-sa`.

- [ ] **Step 1: Update the tests**

In `src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java`, **delete**
`should_reject_a_parquet_source_for_now` (added in Task 9) and add:

```java
    @Test
    void should_reject_a_parquet_source_directory_that_is_not_an_export() throws Exception {
        Path sourceDir = Files.createTempDirectory("dbcopy-parquet-source");
        Path targetFile = Files.createTempDirectory("dbcopy-target-dir").resolve("export.jsonl");

        CommandResult result = execute(
                "-sa", "parquet://" + sourceDir,
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("is not a neo4j-db-copy parquet export");
    }

    @Test
    void should_reject_a_missing_parquet_source_directory() throws Exception {
        Path targetFile = Files.createTempDirectory("dbcopy-target-dir2").resolve("export.jsonl");

        CommandResult result = execute(
                "-sa", "parquet:///tmp/definitely-not-here-" + System.nanoTime(),
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("does not exist");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q test -Dtest=DbCopyCliTest`
Expected: FAIL — the CLI still reports "Parquet is supported as a target only."

- [ ] **Step 3: Wire the reader factory**

In `src/main/java/org/neo4j/dbcopy/io/DataReaderFactory.java`, replace the `PARQUET` branch added
in Task 9 with:

```java
            case PARQUET -> new ParquetDataReader(toParquetDirectoryPath(sourceAddress, "source"));
```

Add `import org.neo4j.dbcopy.parquet.ParquetDataReader;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q test -Dtest=DbCopyCliTest`
Expected: PASS.

- [ ] **Step 5: Run the full unit suite**

Run: `mvn -q test`
Expected: PASS, no regressions.

- [ ] **Step 6: Document it**

In `README.adoc`, update the intro sentence to mention import:

```adoc
A development tool useful to copy the data from a Neo4j database to another Neo4j database, or to and from a JSONL file or a directory of parquet files.
```

Update the parquet paragraph added in Task 9 by appending:

```adoc
A parquet directory can also be used as a source, in which case the property types are read from
the files' footers and nothing is sampled or inferred.
```

Add an example to the Examples block:

```adoc
# parquet -> Neo4j
neo4j-db-copy \
  -sa parquet:///tmp/export \
  -ta neo4j+s://target.example:7687 -tu neo4j -tp -td targetdb
```

- [ ] **Step 7: Commit**

```bash
git add src/main/java/org/neo4j/dbcopy/io/DataReaderFactory.java README.adoc \
        src/test/java/org/neo4j/dbcopy/DbCopyCliTest.java
git commit -m "feat: Accept a parquet directory as a copy source"
```

---

### Task 14: Round-trip integration test

**Files:**
- Test: `src/test/java/org/neo4j/dbcopy/parquet/ParquetRoundTripIT.java`

**Interfaces:**
- Consumes: everything. Produces nothing new.

This is the primary evidence that no type loss occurs: a real graph goes to parquet and back into
a second Neo4j database, and every property is compared for type and value equality against the
source. `BoltWriter` requires APOC, which the shared container already installs.

- [ ] **Step 1: Write the test**

Create `src/test/java/org/neo4j/dbcopy/parquet/ParquetRoundTripIT.java`. Copy the container
declaration from `src/test/java/org/neo4j/dbcopy/DataTransferIT.java:36-60`, using two databases
named `rtsourcedb` and `rttargetdb`.

```java
package org.neo4j.dbcopy.parquet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.bolt.BoltWriter;
import org.neo4j.dbcopy.bolt.BoltSchemaScanner;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("resource")
public class ParquetRoundTripIT {

    public static final Logger LOG = LoggerFactory.getLogger(ParquetRoundTripIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
    private static final String SOURCE_DB = "rtsourcedb";
    private static final String TARGET_DB = "rttargetdb";

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    static Driver driver;
    private Session sourceSession;
    private Session targetSession;

    @BeforeAll
    static void beforeAll() {
        neo4j.start();
        driver = GraphDatabase.driver(neo4j.getBoltUrl(), AUTH_TOKEN);
        driver.executableQuery("CREATE DATABASE " + SOURCE_DB + " IF NOT EXISTS WAIT").execute();
        driver.executableQuery("CREATE DATABASE " + TARGET_DB + " IF NOT EXISTS WAIT").execute();
    }

    @AfterAll
    static void afterAll() {
        if (driver != null) {
            driver.close();
        }
    }

    @BeforeEach
    void setUp() {
        sourceSession = driver.session(SessionConfig.forDatabase(SOURCE_DB));
        targetSession = driver.session(SessionConfig.forDatabase(TARGET_DB));
        sourceSession.run("MATCH (n) DETACH DELETE n").consume();
        targetSession.run("MATCH (n) DETACH DELETE n").consume();
    }

    @Test
    void should_round_trip_every_property_type_without_loss(@TempDir Path dir) {
        sourceSession.run("""
                CREATE (a:Person {
                    name: 'Ada',
                    age: 36,
                    ratio: 2.5,
                    active: true,
                    born: date('1815-12-10'),
                    wakeup: localtime('07:30:00.000000042'),
                    alarm: time('07:30:00+02:00'),
                    seen: localdatetime('2024-01-15T10:30:00.000000042'),
                    createdZoned: datetime('2024-01-15T10:30:00[Europe/Paris]'),
                    createdOffset: datetime('2024-01-15T10:30:00+02:00'),
                    elapsed: duration({months: 1, days: 2, seconds: 3, nanoseconds: 4}),
                    here: point({x: 2.35, y: 48.85}),
                    there: point({x: 1.0, y: 2.0, z: 3.0}),
                    tags: ['a', 'b'],
                    scores: [1, 2, 3],
                    empties: []
                })
                CREATE (b:Person:Employee {name: 'Bob', empNo: 7})
                CREATE (c {note: 'orphan'})
                CREATE (a)-[:KNOWS {since: 2020, weights: [1.5, 2.5]}]->(b)
                """).consume();

        Map<String, Object> before = propertiesOf(sourceSession, "Ada");

        Path export = dir.resolve("export");
        exportToParquet(export);
        importFromParquet(export);

        Map<String, Object> after = propertiesOf(targetSession, "Ada");

        assertThat(after).containsOnlyKeys(before.keySet().toArray(new String[0]));
        before.forEach((name, value) -> {
            if (value instanceof byte[] bytes) {
                assertThat((byte[]) after.get(name)).containsExactly(bytes);
            } else {
                assertThat(after.get(name))
                        .describedAs("property '%s'", name)
                        .isEqualTo(value)
                        .isInstanceOf(value.getClass());
            }
        });

        assertThat(count(targetSession, "MATCH (n) RETURN count(n) AS c")).isEqualTo(3L);
        assertThat(count(targetSession, "MATCH ()-[r]->() RETURN count(r) AS c")).isEqualTo(1L);
        assertThat(count(targetSession, "MATCH (n:Person:Employee) RETURN count(n) AS c")).isEqualTo(1L);
        assertThat(count(targetSession,
                "MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n) AS c")).isEqualTo(1L);
        assertThat(count(targetSession,
                "MATCH (:Person {name: 'Ada'})-[r:KNOWS {since: 2020}]->(:Employee) RETURN count(r) AS c"))
                .isEqualTo(1L);
    }

    private void exportToParquet(Path export) {
        GraphSchema schema = new BoltSchemaScanner(driver, SOURCE_DB).scan(CopyOptions.DEFAULT);
        try (org.neo4j.dbcopy.bolt.BoltReader reader =
                     new NonClosingBoltReader(driver, SOURCE_DB);
             ParquetDataWriter writer = new ParquetDataWriter(export, schema)) {
            writer.writeNodes(reader.readNodes(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(reader.readRelationships(), CopyOptions.DEFAULT).blockLast();
        }
    }

    private void importFromParquet(Path export) {
        try (ParquetDataReader reader = new ParquetDataReader(export);
             DataWriter writer = new NonClosingBoltWriter(driver, TARGET_DB)) {
            writer.writeNodes(reader.readNodes(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(reader.readRelationships(), CopyOptions.DEFAULT).blockLast();
        }
    }

    private static Map<String, Object> propertiesOf(Session session, String name) {
        return session.run("MATCH (n {name: $name}) RETURN properties(n) AS p",
                        org.neo4j.driver.Values.parameters("name", name))
                .single().get("p").asMap();
    }

    private static long count(Session session, String query) {
        return session.run(query).single().get("c").asLong();
    }

    /** {@code BoltReader.close()} closes the shared driver, which the next step still needs. */
    private static final class NonClosingBoltReader extends org.neo4j.dbcopy.bolt.BoltReader {
        NonClosingBoltReader(Driver driver, String database) {
            super(driver, database);
        }

        @Override
        public void close() {
        }
    }

    /** {@code BoltWriter.close()} closes the shared driver, which the assertions still need. */
    private static final class NonClosingBoltWriter extends BoltWriter {
        NonClosingBoltWriter(Driver driver, String database) {
            super(driver, database);
        }

        @Override
        public void close() {
        }
    }
}
```

`BoltReader` and `BoltWriter` are both `public` non-final classes with `public` constructors, so
these subclasses compile as written. If either is made `final` later, switch to closing the driver
after the assertions instead.

- [ ] **Step 2: Run the integration test**

Run: `mvn -q verify -Dit.test=ParquetRoundTripIT`
Expected: PASS, 1 test. Docker must be running.

If `createdZoned` fails on `isEqualTo`, the zone-id reconstruction in `ParquetValues.readDateTime`
is the suspect — Neo4j returns a `ZonedDateTime` whose offset must match the zone rules at that
instant. If `empties` comes back missing rather than as `[]`, the writer wrote null instead of an
empty list for an empty list value; fix `ParquetValues.set` so an empty `List` still calls
`setList` with no elements.

- [ ] **Step 3: Run the whole suite**

Run: `mvn -q verify`
Expected: PASS — all unit and integration tests, including the pre-existing `DataTransferIT` and
`EndpointFactoryIT`.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/org/neo4j/dbcopy/parquet/ParquetRoundTripIT.java
git commit -m "test: Prove Neo4j to parquet to Neo4j round trips without type loss"
```

---

## Deviations from the spec

Recorded here so the spec and the plan do not silently drift:

1. **`scanSchema(CopyOptions)`** rather than `scanSchema(int sampleSize)` — the scan also needs the
   `-enp`/`-erp` exclusion sets. The spec was amended to match.
2. **`PropertyType.Scalar.UNKNOWN`** placeholder for the element type of an all-empty list, so
   merging an empty sample with a later typed list does not falsely conflict. The spec was amended
   to match.
3. **`__dbcopy_type`** on relationship files was added for symmetry with `__dbcopy_labels`; it was
   not in the originally specified set of reserved names.
4. **Writers open lazily** rather than eagerly per label set, so a label set discovered by
   `db.schema.nodeTypeProperties()` but holding no rows produces no empty file.
5. **The unlabelled label set is probed explicitly**, because
   `db.schema.nodeTypeProperties()` reports only labelled nodes. Without this, any graph with an
   unlabelled node would fail at write time.
6. **`ParquetLayout.requireDistinctFileNames`** guards the one file-name collision the escaping
   rule allows (`{A__B}` versus `{A, B}`). Not mentioned in the spec.

