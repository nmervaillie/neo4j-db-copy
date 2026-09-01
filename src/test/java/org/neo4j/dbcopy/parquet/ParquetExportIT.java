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
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

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
                    empties: [],
                    blob: apoc.util.compress('hello world', {})
                })
                CREATE (b:Person:Employee {name: 'Bob', empNo: 7})
                CREATE (c {note: 'orphan'})
                CREATE (a)-[:KNOWS {since: 2020, weights: [1.5, 2.5]}]->(b)
                CREATE (a)-[:WROTE]->(b)
                """).consume();

        Record ids = session.run("""
                MATCH (a:Person {name: 'Ada'})
                MATCH (b:Person:Employee {name: 'Bob'})
                MATCH (a)-[k:KNOWS]->(b)
                RETURN id(a) AS adaId, id(b) AS bobId, id(k) AS knowsId
                """).single();
        long adaId = ids.get("adaId").asLong();
        long bobId = ids.get("bobId").asLong();
        long knowsId = ids.get("knowsId").asLong();

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
            assertThat(rows.getLong(ParquetLayout.ID)).isEqualTo(adaId);
            assertThat(rows.getList(ParquetLayout.LABELS).strings()).containsExactly("Person");
            assertThat(rows.getString("name")).isEqualTo("Ada");
            assertThat(rows.getLong("age")).isEqualTo(36L);
            assertThat(rows.getDouble("ratio")).isEqualTo(2.5);
            assertThat(rows.getBoolean("active")).isTrue();
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
            assertThat(rows.getList("scores").longs().toArray()).containsExactly(1L, 2L, 3L);
            assertThat(rows.getList("empties").isEmpty()).isTrue();
            assertThat(gunzip(rows.getBinary("blob"))).isEqualTo("hello world");
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(export.resolve("relationships/KNOWS.parquet")));
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getLong(ParquetLayout.ID)).isEqualTo(knowsId);
            assertThat(rows.getLong(ParquetLayout.SOURCE_ID)).isEqualTo(adaId);
            assertThat(rows.getLong(ParquetLayout.TARGET_ID)).isEqualTo(bobId);
            assertThat(rows.getString(ParquetLayout.TYPE)).isEqualTo("KNOWS");
            assertThat(rows.getLong("since")).isEqualTo(2020L);
            assertThat(rows.getList("weights").doubles().toArray()).containsExactly(1.5, 2.5);
        }
    }

    /** {@code apoc.util.compress} gzips its input; decompressing proves the raw bytes round-tripped. */
    private static String gunzip(byte[] compressed) throws Exception {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            in.transferTo(out);
            return out.toString(java.nio.charset.StandardCharsets.UTF_8);
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
