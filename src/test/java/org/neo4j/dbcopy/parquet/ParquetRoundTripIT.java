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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
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
    void should_round_trip_every_property_type_without_loss(@TempDir Path dir) throws Exception {
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
        Map<String, Object> beforeRel = relationshipPropertiesOf(sourceSession);

        Path export = dir.resolve("export");
        exportToParquet(export);
        importFromParquet(export);

        Map<String, Object> after = propertiesOf(targetSession, "Ada");
        Map<String, Object> afterRel = relationshipPropertiesOf(targetSession);

        assertThat(after).containsOnlyKeys(before.keySet().toArray(new String[0]));
        assertProperties(before, after);
        assertThat(afterRel).containsOnlyKeys(beforeRel.keySet().toArray(new String[0]));
        assertProperties(beforeRel, afterRel);

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

    private void importFromParquet(Path export) throws Exception {
        try (ParquetDataReader reader = new ParquetDataReader(export);
             DataWriter writer = new NonClosingBoltWriter(driver, TARGET_DB)) {
            writer.writeNodes(reader.readNodes(), CopyOptions.DEFAULT).blockLast();
            writer.writeRelationships(reader.readRelationships(), CopyOptions.DEFAULT).blockLast();
        }
    }

    private static void assertProperties(Map<String, Object> before, Map<String, Object> after) {
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
    }

    private static Map<String, Object> propertiesOf(Session session, String name) {
        return session.run("MATCH (n {name: $name}) RETURN properties(n) AS p",
                        org.neo4j.driver.Values.parameters("name", name))
                .single().get("p").asMap();
    }

    private static Map<String, Object> relationshipPropertiesOf(Session session) {
        return session.run("MATCH ()-[r:KNOWS]->() RETURN properties(r) AS p")
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
