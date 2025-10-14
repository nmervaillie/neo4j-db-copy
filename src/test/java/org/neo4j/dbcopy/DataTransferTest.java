package org.neo4j.dbcopy;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataTransferTest {

    @Test
    void should_copy_nodes_in_batches() {

        TestPublisher<Node> nodesPublishers = TestPublisher.create();
        var writer = new TestDataWriter();

        var transferService = new DataTransfer(
                    new TestDataReader(nodesPublishers.flux(), Flux.empty()),
                    writer,
                    new CopyOptions.Builder().batchSize(3).build());

        StepVerifier.create(transferService.copyAllNodesAndRels())
                .then(() -> nodesPublishers.next(node(1)))
                .then(() -> assertThat(writer.writtenNodes).isEmpty())
                .then(() -> nodesPublishers.next(node(2), node(3), node(4)))
                .then(() -> assertThat(writer.writtenNodes).containsExactly(node(1), node(2), node(3)))
                .then(nodesPublishers::complete)
                .then(() -> assertThat(writer.writtenNodes).containsExactly(node(1), node(2), node(3), node(4)))
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void should_copy_relationships_in_batches() {

        TestPublisher<Relationship> relsPublishers = TestPublisher.create();
        var writer = new TestDataWriter();

        var transferService = new DataTransfer(
                new TestDataReader(Flux.empty(), relsPublishers.flux()),
                writer,
                new CopyOptions.Builder().batchSize(3).build());

        StepVerifier.create(transferService.copyAllNodesAndRels())
                .then(() -> relsPublishers.next(rel(1)))
                .then(() -> assertThat(writer.writtenNodes).isEmpty())
                .then(() -> relsPublishers.next(rel(2), rel(3), rel(4)))
                .then(() -> assertThat(writer.writtenRelationships).containsExactly(rel(1), rel(2), rel(3)))
                .then(relsPublishers::complete)
                .then(() -> assertThat(writer.writtenRelationships).containsExactly(rel(1), rel(2), rel(3), rel(4)))
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    void should_handle_empty_streams() {

        TestDataWriter dataWriter = new TestDataWriter();
        var transferService = new DataTransfer(
                new TestDataReader(Flux.empty(), Flux.empty()),
                dataWriter,
                new CopyOptions.Builder().batchSize(3).build());

        StepVerifier.create(transferService.copyAllNodesAndRels())
                .expectNext(0L)
                .verifyComplete();

        assertThat(dataWriter.writtenNodes).isEmpty();
        assertThat(dataWriter.writtenRelationships).isEmpty();
    }

    private Node node(int id) {
        return new InternalNode(id);
    }

    private Relationship rel(int id) {
        return new InternalRelationship(id, 0, 0, "foo");
    }

    static class TestDataReader implements DataReader {
        private final Flux<Node> nodes;
        private final Flux<Relationship> relationships;

        public TestDataReader(Flux<Node> nodes, Flux<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }

        @Override
        public long getTotalNodeCount() {
            return 0;
        }

        @Override
        public long getTotalRelationshipCount() {
            return 0;
        }

        @Override
        public Flux<Node> readNodes() {
            return nodes;
        }

        @Override
        public Flux<Relationship> readRelationships() {
            return relationships;
        }
    }

    static class TestDataWriter implements DataWriter {

        List<Node> writtenNodes = new ArrayList<>();
        List<Relationship> writtenRelationships = new ArrayList<>();

        @SuppressWarnings("deprecation")
        @Override
        public Flux<MappingContext.Mapping> writeNodes(List<Node> nodes, CopyOptions copyOptions) {
            var mapping = nodes.stream().map(n -> {
                writtenNodes.add(n);
                return new MappingContext.Mapping(n.id(), n.id() + 1000);
            });
            return Flux.fromStream(mapping);
        }

        @Override
        public Mono<Long> writeRelationships(List<Relationship> relationships, MappingContext mappingContext, CopyOptions copyOptions) {
            writtenRelationships.addAll(relationships);
            return Mono.just((long) relationships.size());
        }
    }
}
