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
