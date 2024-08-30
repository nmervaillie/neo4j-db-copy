package org.neo4j.dbcopy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class DataTransferTest {

    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");
//    private static final Config DRIVER_CONFIG = Config.builder()
//            .withMaxConnectionPoolSize(4)
//            .withEventLoopThreads(4 * 2)
////			.withFetchSize(50)
//            .build();
    public static final String SOURCE_DB = "sourcedb";
    public static final String TARGET_DB = "targetdb";

    Driver driver = GraphDatabase.driver("bolt://localhost", AUTH_TOKEN);

    Session sourceSession = driver.session(SessionConfig.forDatabase(SOURCE_DB));
    private final Session targetSession = driver.session(SessionConfig.forDatabase(TARGET_DB));

    @BeforeEach
    void setUp() {
        sourceSession.run("MATCH (n) DETACH DELETE n;").consume();
        targetSession.run("MATCH (n) DETACH DELETE n;").consume();
    }

    @AfterEach
    void tearDown() {
        sourceSession.close();
        targetSession.close();
        driver.close();
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

        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, CopyOptions.DEFAULT);
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

        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.asMap()).containsExactly(Map.entry("prop", 123L));
    }

    @Test
    void should_copy_node_with_several_labels() {

        sourceSession.run("CREATE (one:NodeOne:NodeTwo)").consume();

        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, CopyOptions.DEFAULT);
        dataTransfer.copyAllNodesAndRels().block();

        List<Node> nodes = getAllNodes();
        assertThat(nodes).hasSize(1);
        Node node = nodes.get(0);
        assertThat(node.labels()).containsExactlyInAnyOrder("NodeOne", "NodeTwo");
    }

    @Test
    void should_copy_nodes_and_relationships() {

        sourceSession.run("CREATE (one:NodeOne)-[:TO]->(two:NodeTwo)").consume();

        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, CopyOptions.DEFAULT);
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

        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, CopyOptions.DEFAULT);
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
        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, copyOption);

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
        DataTransfer dataTransfer = new DataTransfer(driver, SOURCE_DB, driver, TARGET_DB, copyOption);

        dataTransfer.copyAllNodesAndRels().block();

        Relationship rel = getAllPaths().get(0).relationships().iterator().next();
        assertThat(rel.asMap()).containsEntry("prop1", "value1").containsEntry("prop2", "value2");
        assertThat(rel.asMap()).doesNotContainKeys("prop3");
    }
}
