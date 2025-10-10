package org.neo4j.dbcopy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MappingContext {

    public record Mapping(long sourceNodeId, long targetNodeId){}

    private static Map<Long, Long> MAPPINGS;

    public MappingContext(int initialCapacity) {
        MAPPINGS = new ConcurrentHashMap<>(initialCapacity);
    }

    public MappingContext add(List<Mapping> mappings) {
        for (Mapping mapping : mappings) {
            MAPPINGS.put(mapping.sourceNodeId, mapping.targetNodeId);
        }
        return this;
    }

    public Long get(Long sourceId) {
        var targetId = MAPPINGS.get(sourceId);
        if (targetId == null) {
            throw new IllegalStateException("Unable to find source node with id " + sourceId);
        }
        return targetId;
    }
}
