# Parquet import/export design

Date: 2026-09-01

## Goal

Add Parquet as a third endpoint kind alongside Bolt and JSONL, usable as both source
and target. A Neo4j -> Parquet -> Neo4j round trip must return every property with its
original Neo4j type and value, with no silent loss.

Parquet needs a column schema before the first row, which Neo4j does not provide.
The schema is therefore inferred by sampling the source graph. That inference is the
one experimental part of this feature: it can be wrong when the graph is not
normalized, and when it is wrong the export fails rather than writing bad data.

## Decisions

| Question | Decision |
|---|---|
| Java baseline | Bump the project from 17 to 21. Hardwood requires 21+. |
| Parquet library | `dev.hardwood:hardwood-core:1.1.0.Beta1` via `hardwood-bom`, on Maven Central. |
| CLI surface | New `parquet:` URI scheme, valid as both `-sa` and `-ta`. |
| Direction | Export and import. |
| File layout | One file per distinct node label set, one per relationship type. |
| Schema source | New `DataReader.scanSchema(int)` capability. |
| Scan depth | Constant sample size, 1000 rows per label set / relationship type. |
| Type mismatch at write time | Fail fast with a message naming entity, property, expected and actual type. |
| Type mapping | Native Parquet logical types where they are exactly lossless; structs everywhere else. |
| Node identity | `id()`, not `elementId()` — matches the JSONL export. |
| Reserved column names | `__dbcopy_`-prefixed, so collision with real properties is practically impossible. |
| Manifest | No `_schema.json`. The Neo4j type map goes in each file's Parquet footer. |

## CLI

```bash
# Neo4j -> Parquet
neo4j-db-copy -sa neo4j+s://src:7687 -su neo4j -sp -sd movies \
              -ta parquet:///tmp/export

# Parquet -> Neo4j
neo4j-db-copy -sa parquet:///tmp/export \
              -ta neo4j+s://dst:7687 -tu neo4j -tp -td movies
```

`EndpointKind` gains `PARQUET`. `EndpointFactorySupport` gains `toDirectoryPath(URI, String)`
alongside the existing `toJsonFilePath`.

Export target directory: created if absent, **refused if it exists and is non-empty**,
mirroring `JsonDataWriter`'s refusal to overwrite (`JsonDataWriter.java:31`).

Import source directory: must contain at least one of `nodes/` or `relationships/`, and
every file read must carry `neo4j.dbcopy.version` in its footer. Otherwise:
`"<path> is not a neo4j-db-copy parquet export."`

Before a Bolt-sourced scan, print to stderr:

```
Parquet export is experimental. The graph schema is inferred by sampling 1000 nodes
per label set and 1000 relationships per type. The export will fail if property types
are not consistent (for example 'age' is an Integer on some :Person nodes and a String
on others), or if a property is absent from every sampled row.
```

Not printed for a Parquet source, where the schema is read exactly from footers.

## Layout

```
/tmp/export/
  nodes/
    Person.parquet            __dbcopy_id, __dbcopy_labels, name, age
    Person__Employee.parquet  __dbcopy_id, __dbcopy_labels, name, age, empNo
    _no_labels.parquet
  relationships/
    KNOWS.parquet             __dbcopy_id, __dbcopy_source_id, __dbcopy_target_id,
                              __dbcopy_type, since
```

File names: labels sorted, joined with `__`. Characters illegal in file names are
percent-escaped. Nodes with no labels go to `_no_labels.parquet`.

### Reserved columns

| Column | Type | Repetition |
|---|---|---|
| `__dbcopy_id` | `INT64` | REQUIRED |
| `__dbcopy_labels` | `LIST<STRING>` | REQUIRED (node files) |
| `__dbcopy_source_id` | `INT64` | REQUIRED (relationship files) |
| `__dbcopy_target_id` | `INT64` | REQUIRED (relationship files) |
| `__dbcopy_type` | `STRING` | REQUIRED (relationship files) |

`__dbcopy_labels` and `__dbcopy_type` are redundant with the file name. They are kept so
a single file is self-describing and so a consumer can union a whole directory.
`__dbcopy_type` was not among the reserved names originally specified; it is added for
symmetry with `__dbcopy_labels`.

**All property columns are OPTIONAL.** Sampling cannot prove a property is mandatory, so
declaring REQUIRED would be unsound. Neo4j has no null property value — absent and null
coincide — so OPTIONAL is also an exact representation.

The scan still checks for a real property named `__dbcopy_*` and fails if it finds one.
This should never fire; it is cheap insurance against a silently clobbered identity column.

## Type mapping

Every mapping below is exactly lossless over the Neo4j domain, subject to the two
validated range checks noted.

| Neo4j type | Parquet | Notes |
|---|---|---|
| Boolean | `BOOLEAN` | |
| Integer | `INT64` | |
| Float | `DOUBLE` | Neo4j Float is a 64-bit double. |
| String | `BYTE_ARRAY` + `StringType` | |
| ByteArray | `BYTE_ARRAY`, no logical type | Absence of `StringType` is what distinguishes it from String. |
| Date | `INT32` + `DateType` | Days since epoch. Exact to +/-5.88M years; range-checked. |
| LocalTime | `INT64` + `TimeType(NANOS, isAdjustedToUTC=false)` | Nanos-of-day peaks at 8.64e13, fits exactly. |
| OffsetTime | `STRUCT{nanoOfDay INT64, offsetSeconds INT32}` | No native Parquet type carries a time-of-day plus offset. |
| LocalDateTime | `STRUCT{epochSecond INT64, nano INT32}` | See below. |
| DateTime | `STRUCT{epochSecond INT64, nano INT32, zoneId STRING OPTIONAL, offsetSeconds INT32}` | See below. |
| Duration | `STRUCT{months INT64, days INT64, seconds INT64, nanos INT32}` | Mirrors `IsoDuration` exactly. |
| Point | `STRUCT{srid INT32, x DOUBLE, y DOUBLE, z DOUBLE OPTIONAL}` | Null `z` means a 2D point. |
| List&lt;T&gt; | `LIST<T>` | Element type from the table, recursively. |

### Why LocalDateTime and DateTime are structs

Parquet's `TimestampType` is an INT64. At `NANOS` its domain is 1677-09-21 to
2262-04-11; Neo4j's temporal year range is +/-999999999. At `MICROS` the range is
practically unbounded but the nanosecond digits Neo4j stores are dropped. No native
Parquet timestamp covers Neo4j's domain, so a struct is the only lossless option, and it
keeps these types consistent with OffsetTime, Duration and Point, which are structs
regardless.

`epochSecond` for LocalDateTime is `toEpochSecond(ZoneOffset.UTC)`; at the extreme year
range that is ~3.2e16, comfortably inside INT64.

### Why DateTime keeps a nullable zoneId

Neo4j `DATETIME` surfaces through the driver as either `ZonedDateTime` (zone id) or
`OffsetDateTime` (fixed offset), and the JSONL export already treats these as two
distinct types (`JsonDataWriter.java:158-165`). Null `zoneId` means the value was an
`OffsetDateTime`. Reconstructing from `(epochSecond, nano, zoneId)` via
`ZonedDateTime.ofInstant` is exact, including across DST overlaps, because the original
value maps to a unique instant.

### Empty lists

`[]` in Neo4j is untyped. If every sampled row has an empty list, the declared element
type is arbitrary (STRING) and `[]` still round-trips exactly. A later non-empty
`[1, 2]` trips the fail-fast check, which is the correct outcome — the sample was
unrepresentative.

To keep merging sound, `PropertyType.Scalar` carries an extra `UNKNOWN` member used only
as the element type of an all-empty list. `merge(UNKNOWN, x) == x`, so a sample of
`[]` followed by `[1, 2]` resolves to `LIST<INTEGER>` instead of falsely conflicting with
the STRING placeholder. `UNKNOWN` never reaches Parquet or a footer token: it renders as
`LIST<STRING>` and never appears at the top level.

### Footer metadata

Each file stamps its Neo4j type map into the Parquet footer via
`ParquetFileWriter.keyValueMetadata`, read back through `FileMetaData.keyValueMetadata()`:

```
neo4j.dbcopy.version = "1"
neo4j.recordType     = "node"                       // or "relationship"
neo4j.labels         = ["Person","Employee"]        // node files
neo4j.relationshipType = "KNOWS"                    // relationship files
neo4j.types          = {"name":"STRING","age":"INTEGER","location":"POINT",
                        "born":"DATE_TIME","tags":"LIST<STRING>"}
```

Import reads declared Neo4j types rather than inferring them from the physical schema.
Without this, the four struct shapes would only be tellable apart by their field names,
and `BYTE_ARRAY` would rely on logical-type presence. With it there is no inference at
all on the import path. Serialized as JSON with Jackson, already a dependency.

## Components

### `org.neo4j.dbcopy.schema` (new)

```java
record GraphSchema(List<NodeTypeSchema> nodeTypes, List<RelationshipTypeSchema> relTypes)
record NodeTypeSchema(SortedSet<String> labels, SortedMap<String, PropertyType> properties)
record RelationshipTypeSchema(String type, SortedMap<String, PropertyType> properties)

sealed interface PropertyType {
    enum Scalar implements PropertyType {
        BOOLEAN, INTEGER, FLOAT, STRING, BYTE_ARRAY,
        DATE, LOCAL_TIME, OFFSET_TIME, LOCAL_DATE_TIME, DATE_TIME, DURATION, POINT
    }
    record ListOf(PropertyType element) implements PropertyType {}
}
```

`Neo4jTypeInference` maps a driver `Value` to a `PropertyType`, merges observations, and
throws on conflict. It also parses and renders the footer type tokens. This is the single
type-dispatch point in the feature: the scanner, the writer and the reader all go through
it, so schema and rows cannot disagree about what a type is.

### `BoltSchemaScanner` (new, in `bolt`)

```cypher
-- label sets, exhaustive and cheap
CALL db.schema.nodeTypeProperties() YIELD nodeLabels
WITH DISTINCT nodeLabels RETURN nodeLabels

-- per label set; the label predicate uses the label index, size() pins the exact set
MATCH (n:`A`:`B`) WHERE size(labels(n)) = 2 WITH n LIMIT $sample RETURN n

-- relationship types
CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType

-- per relationship type
MATCH ()-[r:`T`]->() WITH r LIMIT $sample RETURN r
```

Label sets that the procedure reports but that no longer have nodes yield no rows; those
produce no file rather than an empty one.

**Deviation from SkemaDok:** SkemaDok calls `valueType()` in Cypher and parses the
resulting type strings. This design returns the sampled rows and infers types Java-side
through `Neo4jTypeInference`. That removes a string parser, drops the dependency on
`valueType()`, and guarantees the scan and the writer agree because they share one
code path.

### `DataReader.scanSchema`

```java
default GraphSchema scanSchema(CopyOptions copyOptions) {
    throw new IllegalArgumentException(
        "A parquet target requires a bolt/neo4j or parquet source.");
}
```

`CopyOptions` is passed rather than a bare sample size because the scan also needs the
`-enp`/`-erp` exclusion sets.

Implemented by `BoltReader` (delegating to `BoltSchemaScanner`) and by
`ParquetDataReader` (reading footers exactly, no sampling). `JsonDataReader` keeps the
default throw.

`DataWriterFactory` gains `needsSchema(URI)`; `createWriter` gains a `GraphSchema`
parameter, null when not needed. `DbCopyCli`:

```java
reader = readerFactory.createReader(...);
GraphSchema schema = writerFactory.needsSchema(targetAddress)
        ? reader.scanSchema(copyOptions.schemaSampleSize())
        : null;
writer = writerFactory.createWriter(..., schema);
```

`CopyOptions` gains `schemaSampleSize()`, default 1000, with no CLI flag yet — the same
pattern as the existing unexposed `batchSize` (`CopyOptions.java:44`). That is the hook
for making it configurable later.

Properties excluded via `-enp`/`-erp` are filtered during the scan, so they never enter
the Parquet schema.

### `org.neo4j.dbcopy.parquet` (new)

- `ParquetLayout` — reserved column name constants, file naming and escaping, directory validation
- `ParquetSchemaMapper` — `NodeTypeSchema`/`RelationshipTypeSchema` -> hardwood `FileSchema`
- `ParquetFooter` — read and write the `neo4j.*` footer keys
- `ParquetDataWriter` — `DataWriter`
- `ParquetDataReader` — `DataReader`

Verified against the 1.1.0.Beta1 jar (`FileSchema$Builder`, `RowWriter`, `StructBuilder`,
`ParquetFileReader`, `FileMetaData`, `RepetitionType`, `PhysicalType`, `LogicalType$*`):

```java
FileSchema schema = FileSchema.builder("Person")
    .addColumn("__dbcopy_id", PhysicalType.INT64, RepetitionType.REQUIRED)
    .list("__dbcopy_labels", RepetitionType.REQUIRED,
          e -> e.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                           new LogicalType.StringType()))
    .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
               new LogicalType.StringType())
    .struct("born", RepetitionType.OPTIONAL, s -> s
        .addColumn("epochSecond", PhysicalType.INT64, RepetitionType.REQUIRED)
        .addColumn("nano", PhysicalType.INT32, RepetitionType.REQUIRED)
        .addColumn("zoneId", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                   new LogicalType.StringType())
        .addColumn("offsetSeconds", PhysicalType.INT32, RepetitionType.REQUIRED))
    .build();
```

`WriterConfig`: `codec(CompressionCodec.ZSTD)`. `zstd-jni` is a compile-scope transitive
of `hardwood-core`, so no extra dependency. `brotli4j` and `lz4-java` are excluded from
the shaded jar to keep the executable jar from carrying unused native libraries.

### Export path

Nodes arrive in arbitrary label order, so one `ParquetFileWriter` per label set is opened
and all stay open until nodes finish; relationship writers open afterwards. Concurrent
open file handles equal the number of label sets, then the number of relationship types.
Acceptable for a development tool, recorded as a known limit.

`writeNodes`/`writeRelationships` keep the existing `buffer(batchSize).concatMap(...)`
shape, so `ProgressBar` needs no change.

Failure message on mismatch:

```
Parquet export failed: node 4711 (:Person:Employee) has property 'age' of type STRING,
but the schema scan inferred INTEGER from the sampled nodes. Property types must be
consistent for parquet export.
```

Same shape for a property absent from the schema, and for the one range check (a Date
beyond +/-5.88M years, which `INT32` days cannot hold). Files already written stay on
disk; the message states the directory is incomplete.

### Import path

- `getTotalNodeCount()` / `getTotalRelationshipCount()` sum `FileMetaData.numRows()`
  across footers — metadata only, no row scan. Cheaper than the JSONL reader, which
  counts by parsing every line (`JsonDataReader.java:84`).
- `readNodes()` concatenates per-file row streams in sorted file order for determinism.
  Each file's footer supplies labels and the type map; `RowReader` values become driver
  `Value`s via `Neo4jTypeInference`.
- `BoltWriter` needs no change: it already remaps source ids through `nodeIdMappings`
  (`BoltWriter.java:33`), so a Parquet `__dbcopy_id` behaves exactly as a JSONL `id`.
- Parquet -> Parquet and Parquet -> JSONL work for free.

### Refactor: shared entity implementations

`JsonNodeEntity` and `JsonRelationshipEntity` are ~200 lines of `Node`/`Relationship`
boilerplate at the bottom of `JsonDataReader.java`. They move to
`org.neo4j.dbcopy.io.NodeRecord` and `org.neo4j.dbcopy.io.RelationshipRecord` and are
shared by both file-based readers. `JsonDataReader` shrinks accordingly. This is the only
refactor in scope; no unrelated cleanup.

## Testing

- `Neo4jTypeInferenceTest` — every driver value type to `PropertyType` and back; footer
  token round trip; conflict detection
- `ParquetSchemaMapperTest` — schema to `FileSchema`, covering all five structs, lists,
  the reserved columns, and the `__dbcopy_*` collision failure
- `ParquetLayoutTest` — file naming, label sorting, escaping, `_no_labels`, non-empty
  directory refusal, unrecognised import directory
- `ParquetDataWriterTest` — write a temp directory from a hand-built schema, read back
  with hardwood's `RowReader`, assert values; both failure messages; both range checks
- `ParquetDataReaderTest` — read a fixture directory; counts from footers; missing-footer
  rejection
- `ParquetExportIT` — testcontainers Neo4j, graph covering every type in the mapping
  table plus multi-label nodes, an unlabelled node, empty lists, 2D and 3D points, and
  both DateTime flavours; run the CLI, read the output back
- `ParquetRoundTripIT` — Neo4j -> Parquet -> Neo4j, asserting every property returns
  type-identical. This is the primary evidence that no type loss occurs.

## Risks

- **Hardwood 1.1.0.Beta1 is a beta.** API verified against the jar, but behaviour is not
  battle-tested. Isolated behind `ParquetSchemaMapper` / `ParquetDataWriter` /
  `ParquetDataReader`.
- **Sampling can be unrepresentative.** Mitigated by fail-fast plus the experimental
  warning; not eliminated. Raising `schemaSampleSize` is the escape hatch.
- **Label-set explosion.** A graph with many label combinations produces many files and
  many concurrently open handles.
- **Java 21 bump** is a breaking change for anyone running the CLI on 17.
- **Bolt import still needs APOC** (`apoc.create.addLabels`, `apoc.create.relationship`
  in `BoltWriter`). Pre-existing, unchanged by this work.

## Out of scope

Indexes, constraints, users, data deletion — unchanged from the tool's existing scope.
Partitioned or multi-file-per-type output. Exposing the sample size as a CLI flag.
`_schema.json` manifest.
