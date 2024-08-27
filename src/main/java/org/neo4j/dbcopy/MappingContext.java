package org.neo4j.dbcopy;

import java.util.HashMap;
import java.util.List;

public class MappingContext {

    record Mapping(long sourceNodeId, long targetNodeId){}

    private static HashMap<Long, Long> MAPPINGS;

    public MappingContext(int initialCapacity) {
        MAPPINGS = new HashMap<>(initialCapacity);
    }

    MappingContext add(List<Mapping> mappings) {
        for (Mapping mapping : mappings) {
            MAPPINGS.put(mapping.sourceNodeId, mapping.targetNodeId);
        }
        return this;
    }

    Long get(Long sourceId) {
        var targetId = MAPPINGS.get(sourceId);
        if (targetId == null) {
            throw new IllegalStateException("Unable to find source node with id " + sourceId);
        }
        return targetId;
    }
}
