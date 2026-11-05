package org.neo4j.dbcopy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.bolt.BoltReader;
import org.neo4j.dbcopy.bolt.BoltWriter;
import org.neo4j.dbcopy.json.JsonDataReader;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
public class IndexAndConstraintCopyIT {

    public static final Logger LOG = LoggerFactory.getLogger(IndexAndConstraintCopyIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
    private static final String SOURCE_DB = "schemadb";
    private static final String TARGET_DB = "schematargetdb";

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    static Driver driver;
    private final Session sourceSession = driver.session(SessionConfig.forDatabase(SOURCE_DB));
    private final Session targetSession = driver.session(SessionConfig.forDatabase(TARGET_DB));
    private BoltReader dataReader;
    private BoltWriter dataWriter;

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
        clearDatabase(sourceSession);
        clearDatabase(targetSession);
        dataReader = new BoltReader(driver, SOURCE_DB);
        dataWriter = new BoltWriter(driver, TARGET_DB);
    }

    @AfterEach
    void tearDown() {
        sourceSession.close();
        targetSession.close();
    }

    private static void runEach(Session session, String statements) {
        for (String statement : statements.split("\n")) {
            if (!statement.isBlank()) {
                session.run(statement).consume();
            }
        }
    }

    private static void clearDatabase(Session session) {        session.run("MATCH (n) DETACH DELETE n").consume();
        session.run("SHOW CONSTRAINTS").list(record -> record.get("name").asString())
                .forEach(name -> session.run("DROP CONSTRAINT `" + name + "` IF EXISTS").consume());
        session.run("SHOW INDEXES").list().stream()
                .filter(record -> record.get("owningConstraint").isNull())
                .filter(record -> !"LOOKUP".equals(record.get("type").asString()))
                .map(record -> record.get("name").asString())
                .forEach(name -> session.run("DROP INDEX `" + name + "` IF EXISTS").consume());
    }

    @Test
    void should_copy_indexes_and_constraints() {
        runEach(sourceSession, """
                CREATE CONSTRAINT person_name_unique FOR (n:Person) REQUIRE n.name IS UNIQUE
                CREATE CONSTRAINT person_email_exists FOR (n:Person) REQUIRE n.email IS NOT NULL
                CREATE CONSTRAINT person_name_key FOR (n:Person) REQUIRE (n.name, n.email) IS NODE KEY
                CREATE CONSTRAINT worked_at_exists FOR ()-[r:WORKED_AT]-() REQUIRE r.since IS NOT NULL
                CREATE CONSTRAINT person_id_type FOR (n:Person) REQUIRE n.id IS :: STRING
                CREATE RANGE INDEX person_age FOR (n:Person) ON (n.age)
                CREATE TEXT INDEX person_bio FOR (n:Person) ON (n.bio)
                CREATE FULLTEXT INDEX person_search FOR (n:Person) ON EACH [n.name, n.bio]
                """);
        sourceSession.run("""
                CREATE (a:Person {name: 'Alice', email: 'a@example.com', id: '1', age: 30, bio: 'developer'})
                CREATE (b:Company {name: 'Neo4j'})
                CREATE (a)-[:WORKED_AT {since: 2020}]->(b)
                """).consume();

        List<String> ddl = dataReader.readIndexAndConstraintDdl();

        new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT)
                .copyAllNodesAndRels()
                .block();
        dataWriter.applyIndexAndConstraintDdl(ddl);

        List<String> targetDdl = new BoltReader(driver, TARGET_DB).readIndexAndConstraintDdl();
        assertThat(targetDdl).containsExactlyInAnyOrderElementsOf(ddl);
        assertThat(targetSession.run("MATCH (n:Person) RETURN count(n) AS count").single().get("count").asLong())
                .isEqualTo(1);
        assertThat(targetSession.run("MATCH ()-[r:WORKED_AT]->() RETURN count(r) AS count").single().get("count").asLong())
                .isEqualTo(1);
    }

    @Test
    void should_reject_copy_indexes_for_jsonl_source(@TempDir Path tempDir) throws Exception {
        Path jsonFile = tempDir.resolve("graph.jsonl");
        Files.writeString(jsonFile, "");

        try (JsonDataReader jsonReader = new JsonDataReader(jsonFile)) {
            assertThatThrownBy(jsonReader::readIndexAndConstraintDdl)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("--copy-indexes");
        }
    }
}
