package org.neo4j.dbcopy;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbcopy.bolt.BoltReader;
import org.neo4j.dbcopy.bolt.BoltWriter;
import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.io.DataReaderFactory;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.dbcopy.io.DataWriterFactory;
import org.neo4j.dbcopy.json.JsonDataReader;
import org.neo4j.dbcopy.json.JsonDataWriter;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
public class DataTransferIT {

    public static final Logger LOG = LoggerFactory.getLogger(DataTransferIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
    private static final String SOURCE_DB = "sourcedb";
    private static final String TARGET_DB = "targetdb";

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    static Driver driver;
    private final Session sourceSession = driver.session(SessionConfig.forDatabase(SOURCE_DB));
    private final Session targetSession = driver.session(SessionConfig.forDatabase(TARGET_DB));
    DataReader dataReader;
    DataWriter dataWriter;

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
        sourceSession.run("MATCH (n) DETACH DELETE n;").consume();
        targetSession.run("MATCH (n) DETACH DELETE n;").consume();
        dataReader = new BoltReader(driver, SOURCE_DB);
        dataWriter = new BoltWriter(driver, TARGET_DB);
    }

    @AfterEach
    void tearDown() {
        sourceSession.close();
        targetSession.close();
    }

    private List<Node> getAllNodes() {
        return targetSession.run("MATCH (n) RETURN n").list((rec) -> rec.get(0).asNode());
    }

    private List<Path> getAllPaths() {
        return targetSession.run("MATCH p = (n)-[]->() RETURN p").list((rec) -> rec.get("p").asPath());
    }

    @Test
    void should_copy_a_single_node() {

        sourceSession.run("CREATE (one:NodeOne) SET one.prop = 123").consume();

        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.labels()).containsExactly("NodeOne");
        assertThat(node.asMap()).containsExactly(Map.entry("prop", 123L));
    }

    @Test
    void should_copy_node_properties() {

        sourceSession.run("CREATE (one:NodeOne) SET one.prop=123").consume();

        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.asMap()).containsExactly(Map.entry("prop", 123L));
    }

    @Test
    void should_copy_node_with_several_labels() {

        sourceSession.run("CREATE (one:NodeOne:NodeTwo)").consume();

        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.labels()).containsExactlyInAnyOrder("NodeOne", "NodeTwo");
    }

    @Test
    void should_copy_nodes_and_relationships() {

        sourceSession.run("CREATE (one:NodeOne)-[:TO]->(two:NodeTwo)").consume();

        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Path> paths = getAllPaths();
        assertThat(paths).hasSize(1);
        Path path = paths.get(0);
        assertThat(path.start().labels()).containsExactly("NodeOne");
        assertThat(path.relationships().iterator().next().type()).isEqualTo("TO");
        assertThat(path.end().labels()).containsExactly("NodeTwo");
    }

    @Test
    void should_copy_relationship_properties() {

        sourceSession.run("CREATE (one:NodeOne)-[to:TO]->(two:NodeTwo) SET to.value='foo'").consume();

        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        Relationship rel = getAllPaths().get(0).relationships().iterator().next();
        assertThat(rel.type()).isEqualTo("TO");
        assertThat(rel.asMap()).containsExactly(Map.entry("value", "foo"));
    }

    @Test
    void should_copy_node_with_excluded_properties() {
        sourceSession.run("CREATE (one:NodeOne {prop1: 'value1', prop2: 'value2', prop3: 'value3'})").consume();

        CopyOptions copyOption = new CopyOptions.Builder()
                .excludeNodeProperties(Set.of("prop2"))
                .build();
        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, copyOption);

        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.asMap()).containsEntry("prop1", "value1").containsEntry("prop3", "value3");
        assertThat(node.asMap()).doesNotContainKeys("prop2");
    }

    @Test
    void should_copy_relationship_with_excluded_properties() {
        sourceSession.run("CREATE (one:NodeOne)-[rel:TO {prop1: 'value1', prop2: 'value2', prop3: 'value3'}]->(two:NodeTwo)").consume();

        CopyOptions copyOption = new CopyOptions.Builder()
                .excludeRelationshipProperties(Set.of("prop3"))
                .build();
        DataTransfer dataTransfer = new DataTransfer(dataReader, dataWriter, copyOption);

        dataTransfer.copyAllNodesAndRels().block();

        Relationship rel = getAllPaths().get(0).relationships().iterator().next();
        assertThat(rel.asMap()).containsEntry("prop1", "value1").containsEntry("prop2", "value2");
        assertThat(rel.asMap()).doesNotContainKeys("prop3");
    }

    @Test
    void should_round_trip_through_jsonl(@TempDir java.nio.file.Path tempDir) throws Exception {
        sourceSession.run("""
                CREATE (one:NodeOne {name: 'Alice', active: true, tags: ['friend', 'colleague'], created: date('2024-01-15')})
                CREATE (two:NodeTwo {count: 2})
                CREATE (one)-[:TO {since: 2020, close: false}]->(two)
                """).consume();

        java.nio.file.Path jsonFile = tempDir.resolve("graph.jsonl");

        try (JsonDataWriter jsonWriter = new JsonDataWriter(jsonFile)) {
            new DataTransfer(new BoltReader(driver, SOURCE_DB), jsonWriter, CopyOptions.DEFAULT)
                    .copyAllNodesAndRels()
                    .block();
        }

        new DataTransfer(new JsonDataReader(jsonFile), new BoltWriter(driver, TARGET_DB), CopyOptions.DEFAULT)
                .copyAllNodesAndRels()
                .block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(2);
        assertThat(nodes).anySatisfy(node -> {
            if (node.hasLabel("NodeOne")) {
                assertThat(node.asMap()).containsEntry("name", "Alice")
                        .containsEntry("active", true)
                        .containsEntry("tags", List.of("friend", "colleague"));
            }
        });

        Relationship rel = getAllPaths().get(0).relationships().iterator().next();
        assertThat(rel.type()).isEqualTo("TO");
        assertThat(rel.asMap()).containsEntry("since", 2020L).containsEntry("close", false);
    }

    @Test
    void should_fail_reader_construction_when_source_database_does_not_exist() {
        assertThatThrownBy(() -> new DataReaderFactory()
                .createReader(URI.create(neo4j.getBoltUrl()), "neo4j", "password", "missing-source-db"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to open database 'missing-source-db'");
    }

    @Test
    void should_fail_writer_construction_when_target_database_does_not_exist() {
        assertThatThrownBy(() -> new DataWriterFactory()
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "password", "missing-target-db"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to open database 'missing-target-db'");
    }

    @Test
    void should_fail_reader_construction_when_credentials_are_wrong() {
        assertThatThrownBy(() -> new DataReaderFactory()
                .createReader(URI.create(neo4j.getBoltUrl()), "neo4j", "wrong-password", "neo4j"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to authenticate to the Neo4j server");
    }

    @Test
    void should_fail_writer_construction_when_credentials_are_wrong() {
        assertThatThrownBy(() -> new DataWriterFactory()
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "wrong-password", "neo4j"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to authenticate to the Neo4j server");
    }
}
