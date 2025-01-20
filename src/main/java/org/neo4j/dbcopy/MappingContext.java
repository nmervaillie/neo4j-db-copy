package org.neo4j.dbcopy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MappingContext {

    record Mapping(long sourceNodeId, long targetNodeId){}

    private static Map<Long, Long> MAPPINGS;

    public MappingContext(int initialCapacity) {
        MAPPINGS = new ConcurrentHashMap<>(initialCapacity);
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
