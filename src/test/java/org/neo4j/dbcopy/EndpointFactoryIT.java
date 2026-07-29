package org.neo4j.dbcopy;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.dbcopy.io.DataReaderFactory;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.dbcopy.io.DataWriterFactory;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
public class EndpointFactoryIT {

    public static final Logger LOG = LoggerFactory.getLogger(EndpointFactoryIT.class);
    private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "password");

    final static Neo4jContainer neo4j = new Neo4jContainer(DockerImageName.parse("neo4j:5.19.0-enterprise"))
            .withAdminPassword("password")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withReuse(true);

    @BeforeAll
    static void beforeAll() {
        neo4j.start();
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
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "password", "missing-target-db", false))
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
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "wrong-password", "neo4j", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to authenticate to the Neo4j server");
    }

    @Test
    void should_create_target_database_when_requested() throws Exception {
        String newDatabase = "new-db-" + UUID.randomUUID();

        try (DataWriter writer = new DataWriterFactory()
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "password", newDatabase, true)) {
            assertThat(writer).isNotNull();
        }

        var driver = GraphDatabase.driver(neo4j.getBoltUrl(), AUTH_TOKEN);
        try (Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            List<String> databaseNames = systemSession.run("SHOW DATABASES YIELD name")
                    .list(record -> record.get("name").asString());
            assertThat(databaseNames).contains(newDatabase);
        }
        driver.close();
    }

    @Test
    void should_not_fail_when_target_database_already_exists_and_create_is_requested() throws Exception {
        String existingDatabase = "existing-db-" + UUID.randomUUID();

        try (DataWriter writer = new DataWriterFactory()
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "password", existingDatabase, true)) {
            assertThat(writer).isNotNull();
        }

        try (DataWriter writer = new DataWriterFactory()
                .createWriter(URI.create(neo4j.getBoltUrl()), "neo4j", "password", existingDatabase, true)) {
            assertThat(writer).isNotNull();
        }
    }
}
