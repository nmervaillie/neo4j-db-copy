package org.neo4j.dbcopy;

import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;

public interface DataReader {

    Flux<Node> readNodes();

    Flux<Relationship> readRelationships();

    long getTotalNodeCount();

    long getTotalRelationshipCount();

}
