package org.neo4j.dbcopy.json;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

final class JsonIo {

    private JsonIo() {
    }

    static InputStream newInputStream(Path path) throws IOException {
        InputStream inputStream = new BufferedInputStream(Files.newInputStream(path));
        if (isGzip(path)) {
            return new GZIPInputStream(inputStream);
        }
        return inputStream;
    }

    static OutputStream newOutputStream(Path path) throws IOException {
        OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(path));
        if (isGzip(path)) {
            return new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    private static boolean isGzip(Path path) {
        return path.getFileName().toString().toLowerCase().endsWith(".gz");
    }
}
