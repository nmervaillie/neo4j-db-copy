package org.neo4j.dbcopy.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.*;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class JsonDataWriter implements DataWriter, AutoCloseable {

    private final OutputStream outputStream;
    private final JsonGenerator json;

    public JsonDataWriter(Path path) {
        if (Files.exists(path)) {
            throw new IllegalArgumentException("Refusing to overwrite existing JSON export file: " + path +
                    ". Please remove it first or choose a different target path.");
        }
        System.err.println("Writing JSON export file to " + path);
        try {
            this.outputStream = JsonIo.newOutputStream(path);
            JsonFactory factory = new JsonFactory();
            this.json = factory.createGenerator(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            this.json.setPrettyPrinter(null);
            this.json.setRootValueSeparator(new SerializedString(""));
        } catch (IOException e) {
            throw new RuntimeException("Unable to open JSON export file", e);
        }
    }

    @Override
    public Mono<Void> writeNodes(List<Node> nodes, CopyOptions copyOptions) {
        return Mono.fromRunnable(() -> {
            try {
                for (Node node : nodes) {
                    writeNode(node);
                }
                json.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write node batch to JSON export", e);
            }
        });
    }

    @Override
    public Mono<Long> writeRelationships(List<Relationship> relationships, CopyOptions copyOptions) {
        return Mono.fromCallable(() -> {
            for (Relationship relationship : relationships) {
                writeRelationship(relationship);
            }
            json.flush();
            return (long) relationships.size();
        }).onErrorMap(IOException.class, e -> new UncheckedIOException("Failed to write relationship batch to JSON export", e));
    }

    private void writeNode(Node node) throws IOException {
        json.writeStartObject();

        json.writeStringField("recordType", "node");
        json.writeNumberField("id", node.id());

        json.writeArrayFieldStart("labels");
        node.labels().forEach(l -> {
            try {
                json.writeString(l);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        json.writeEndArray();
        writeProperties(node);
        json.writeEndObject();
        json.writeRaw('\n');
    }

    private void writeProperties(MapAccessor nodeOrRelationship) throws IOException {
        json.writeObjectFieldStart("properties");
        for (String key : nodeOrRelationship.keys()) {
            json.writeFieldName(key);
            writeTypedValue(nodeOrRelationship.get(key));
        }
        json.writeEndObject();
    }

    private void writeRelationship(Relationship rel) throws IOException {
        json.writeStartObject();

        json.writeStringField("recordType", "relationship");
        json.writeNumberField("id", rel.id());
        json.writeNumberField("startId", rel.startNodeId());
        json.writeNumberField("endId", rel.endNodeId());
        json.writeStringField("type", rel.type());
        writeProperties(rel);
        json.writeEndObject();
        json.writeRaw('\n');
    }

    private void writeTypedValue(Value value) throws IOException {
        Object o = value.asObject();

        json.writeStartObject();

        if (o == null) {
            json.writeStringField("type", "Null");
            json.writeNullField("value");
        } else if (o instanceof Boolean b) {
            json.writeStringField("type", "Boolean");
            json.writeBooleanField("value", b);
        } else if (o instanceof Long l) {
            json.writeStringField("type", "Integer");
            json.writeNumberField("value", l);
        } else if (o instanceof Double d) {
            json.writeStringField("type", "Float");
            json.writeNumberField("value", d);
        } else if (o instanceof String s) {
            json.writeStringField("type", "String");
            json.writeStringField("value", s);
        } else if (o instanceof byte[] bytes) {
            json.writeStringField("type", "ByteArray");
            json.writeStringField("value", Base64.getEncoder().encodeToString(bytes));
        } else if (o instanceof List<?> list) {
            json.writeStringField("type", "List");
            json.writeArrayFieldStart("value");
            for (Object element : list) {
                writeTypedValue(Values.value(element));
            }
            json.writeEndArray();
        } else if (o instanceof LocalDate date) {
            json.writeStringField("type", "LocalDate");
            json.writeStringField("value", date.toString());
        } else if (o instanceof LocalTime lt) {
            json.writeStringField("type", "LocalTime");
            json.writeStringField("value", lt.toString());
        } else if (o instanceof OffsetTime ot) {
            json.writeStringField("type", "OffsetTime");
            json.writeStringField("value", ot.toString());
        } else if (o instanceof LocalDateTime ldt) {
            json.writeStringField("type", "LocalDateTime");
            json.writeStringField("value", ldt.toString());
        } else if (o instanceof ZonedDateTime zdt) {
            json.writeStringField("type", "ZonedDateTime");
            json.writeStringField("value", zdt.toString());
        } else if (o instanceof OffsetDateTime odt) {
            json.writeStringField("type", "OffsetDateTime");
            json.writeStringField("value", odt.toString());
        } else if (o instanceof IsoDuration dur) {
            json.writeStringField("type", "Duration");
            json.writeObjectFieldStart("value");
            json.writeNumberField("months", dur.months());
            json.writeNumberField("days", dur.days());
            json.writeNumberField("seconds", dur.seconds());
            json.writeNumberField("nanos", dur.nanoseconds());
            json.writeEndObject();
        } else if (o instanceof Point p) {
            json.writeStringField("type", "Point");
            json.writeNumberField("srid", p.srid());
            json.writeObjectFieldStart("value");
            json.writeNumberField("x", p.x());
            json.writeNumberField("y", p.y());
            if (!Double.isNaN(p.z())) json.writeNumberField("z", p.z());
            else json.writeNullField("z");
            json.writeEndObject();
        } else {
            throw unsupportedValue(o);
        }

        json.writeEndObject();
    }

    private IllegalArgumentException unsupportedValue(Object value) {
        String typeName = value == null ? "null" : value.getClass().getName();
        if (value instanceof Map<?, ?>) {
            return new IllegalArgumentException("Unsupported property value type for JSON export: " + typeName + ". Neo4j property maps are not supported.");
        }
        return new IllegalArgumentException("Unsupported property value type for JSON export: " + typeName);
    }

    @Override
    public void close() throws IOException {
        json.close();
        outputStream.close();
    }
}
