package org.neo4j.dbcopy;

import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DataTransfer {

	protected static final Logger LOG = LoggerFactory.getLogger(DataTransfer.class);

    private final DataReader dataReader;
	private final DataWriter dataWriter;
    private final CopyOptions copyOptions;

	public DataTransfer(DataReader dataReader, DataWriter dataWriter, CopyOptions copyOptions) {
		this.dataReader = dataReader;
		this.dataWriter = dataWriter;
        this.copyOptions = copyOptions;
    }

	Mono<Long> copyAllNodesAndRels() {
        ProgressBar nodeProgressBar = new ProgressBar("Nodes", dataReader.getTotalNodeCount());
		ProgressBar relationshipProgressBar = new ProgressBar("Relationships", dataReader.getTotalRelationshipCount());

		return dataWriter.writeNodes(readNodes(), copyOptions)
				.doOnNext(written -> nodeProgressBar.updateProgress(written.intValue()))
				.then(dataWriter.writeRelationships(readRels(), copyOptions)
					.doOnNext(written -> relationshipProgressBar.updateProgress(written.intValue()))
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
}
