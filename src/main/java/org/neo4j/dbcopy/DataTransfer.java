package org.neo4j.dbcopy;

import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

class DataTransfer {

	protected static final Logger LOG = LoggerFactory.getLogger(DataTransfer.class);

	protected static final int WRITER_CONCURRENCY = 4;

    private final DataReader dataReader;
	private final DataWriter dataWriter;
    private final CopyOptions copyOptions;

	public DataTransfer(DataReader dataReader, DataWriter dataWriter, CopyOptions copyOptions) {
		this.dataReader = dataReader;
		this.dataWriter = dataWriter;
        this.copyOptions = copyOptions;
    }

	Mono<Long> copyAllNodesAndRels() {
        var batchSize = copyOptions.batchSize();
        ProgressBar nodeProgressBar = new ProgressBar("Nodes", dataReader.getTotalNodeCount());
		ProgressBar relationshipProgressBar = new ProgressBar("Relationships", dataReader.getTotalRelationshipCount());

		return readNodes()
				// ideally we should filter out properties to exclude here
				// but the nodes are immutable and that would require duplicating the node data structure here
				// which I don't want to do (yet)
				.buffer(batchSize)
				.doOnNext(batch -> nodeProgressBar.updateProgress(batch.size()))
				.flatMap(this::writeNodes, WRITER_CONCURRENCY)
				.then(readRels()
					.buffer(batchSize)
					.doOnNext(batch -> relationshipProgressBar.updateProgress(batch.size()))
					.flatMap(this::writeRels, 1)
					.reduce(0L, Long::sum)
				)
				.doOnSuccess(it -> LOG.info("Relationships writing complete - {} relationships written", it));
	}

	private Flux<Node> readNodes() {
		return dataReader.readNodes();
	}

	private Flux<Relationship> readRels() {
		return dataReader.readRelationships();
	}

	private Mono<Void> writeNodes(List<Node> nodes) {
		return dataWriter.writeNodes(nodes, copyOptions);
	}

	private Mono<Long> writeRels(List<Relationship> relationships) {
		return dataWriter.writeRelationships(relationships, copyOptions);
	}

}
