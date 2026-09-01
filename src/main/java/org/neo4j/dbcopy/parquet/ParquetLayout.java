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
