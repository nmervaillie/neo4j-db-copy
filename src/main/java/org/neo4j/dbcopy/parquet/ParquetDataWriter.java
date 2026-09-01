package org.neo4j.dbcopy.parquet;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.StructBuilder;
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
import reactor.core.scheduler.Schedulers;

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
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Writes a graph as one parquet file per node label set and per relationship type. The schema must
 * be known up front, which is why it is scanned from the source before this writer is created.
 */
public final class ParquetDataWriter implements DataWriter {

    /**
     * The writer keeps one open {@link ParquetFileWriter} per node label set (and, separately, one
     * per relationship type) for the whole node/relationship phase, and each one buffers up to
     * {@code rowGroupBufferTargetBytes} before flushing a row group. Hardwood's default is 128 MiB,
     * and the shipped executable jar runs with {@code -Xmx1G} (see pom.xml). A graph with ten or
     * more label sets open at once could exceed the heap on buffers alone, so bound each writer's
     * buffer low enough that dozens of concurrently open writers still comfortably fit under 1 GiB.
     */
    private static final long ROW_GROUP_BUFFER_TARGET_BYTES = 16L << 20; // 16 MiB

    private static final WriterConfig CONFIG = WriterConfig.builder()
            .codec(CompressionCodec.ZSTD)
            .rowGroupBufferTargetBytes(ROW_GROUP_BUFFER_TARGET_BYTES)
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
        int sampleSize = copyOptions.schemaSampleSize();
        return nodes.buffer(copyOptions.batchSize())
                .concatMap(batch -> Mono.fromCallable(() -> {
                    for (Node node : batch) {
                        writeNode(node, copyOptions.getNodePropertiesToExclude(), sampleSize);
                    }
                    return (long) batch.size();
                }).subscribeOn(Schedulers.boundedElastic()));
    }

    @Override
    public Flux<Long> writeRelationships(Flux<Relationship> relationships, CopyOptions copyOptions) {
        int sampleSize = copyOptions.schemaSampleSize();
        return relationships.buffer(copyOptions.batchSize())
                .concatMap(batch -> Mono.fromCallable(() -> {
                    for (Relationship relationship : batch) {
                        writeRelationship(relationship, copyOptions.getRelationshipPropertiesToExclude(), sampleSize);
                    }
                    return (long) batch.size();
                }).subscribeOn(Schedulers.boundedElastic()))
                .doFirst(this::closeNodeWriters);
    }

    private void writeNode(Node node, Set<String> excluded, int sampleSize) {
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
        requireKnownProperties(context, node.keys(), writer.types(), excluded, sampleSize);
        writer.writeRow(context, row -> {
            row.setLong(ParquetLayout.ID, node.id());
            row.setList(ParquetLayout.LABELS, list -> labels.forEach(list::addString));
            setProperties(row, writer.types(), node::get, excluded);
        });
    }

    private void writeRelationship(Relationship relationship, Set<String> excluded, int sampleSize) {
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
        requireKnownProperties(context, relationship.keys(), writer.types(), excluded, sampleSize);
        writer.writeRow(context, row -> {
            row.setLong(ParquetLayout.ID, relationship.id());
            row.setLong(ParquetLayout.SOURCE_ID, relationship.startNodeId());
            row.setLong(ParquetLayout.TARGET_ID, relationship.endNodeId());
            row.setString(ParquetLayout.TYPE, relationship.type());
            setProperties(row, writer.types(), relationship::get, excluded);
        });
    }

    /**
     * Property discovery is sampled, not exhaustive: {@code BoltSchemaScanner} unions property keys
     * over only {@code sampleSize} rows per label set/relationship type. A property that happens to
     * be absent from every sampled row would otherwise be silently dropped from every entity on
     * export, so fail fast instead the moment an entity carries a property the schema scan never saw.
     */
    private static void requireKnownProperties(String context, Iterable<String> keys,
                                                SortedMap<String, PropertyType> types,
                                                Set<String> excluded, int sampleSize) {
        for (String key : keys) {
            if (!excluded.contains(key) && !types.containsKey(key)) {
                throw new IllegalArgumentException("Parquet export failed: " + context + " has property '"
                        + key + "', which the schema scan did not see in its sample of "
                        + sampleSize + " rows. Parquet export requires every property to appear in the sample.");
            }
        }
    }

    private static void setProperties(StructBuilder row,
                                      SortedMap<String, PropertyType> types,
                                      Function<String, Value> lookup,
                                      Set<String> excluded) {
        types.forEach((name, type) -> {
            Value value = excluded.contains(name) ? null : lookup.apply(name);
            ParquetValues.set(row, name, type, value);
        });
    }

    private static String labelKey(SortedSet<String> labels) {
        return String.join(":", labels);
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
        try {
            closeNodeWriters();
        } finally {
            closeAll(relWriters);
        }
    }

    private static void closeAll(Map<String, EntityWriter> writers) {
        RuntimeException failure = null;
        for (EntityWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
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

        private static EntityWriter create(Path path, FileSchema schema,
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

        void writeRow(String context, Consumer<StructBuilder> row) {
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
