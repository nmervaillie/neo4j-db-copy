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
            try {
                rows.close();
            } finally {
                closeQuietly(reader);
            }
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
