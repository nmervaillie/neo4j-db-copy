package org.neo4j.dbcopy;

import org.neo4j.dbcopy.MappingContext.Mapping;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface DataWriter {

    Flux<Mapping> writeNodes(List<Node> nodes, CopyOptions copyOptions);

    Mono<Long> writeRelationships(List<Relationship> relationships, MappingContext mappingContext, CopyOptions copyOptions);
}
