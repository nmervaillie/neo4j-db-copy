package org.neo4j.dbcopy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MappingContext {

    public record Mapping(long sourceNodeId, long targetNodeId){}

    private final Map<Long, Long> mappings;

    public MappingContext(int initialCapacity) {
        this.mappings = new ConcurrentHashMap<>(initialCapacity);
    }

    public MappingContext add(List<Mapping> mappings) {
        for (Mapping mapping : mappings) {
            add(mapping);
        }
        return this;
    }

    public MappingContext add(Mapping mapping) {
        mappings.put(mapping.sourceNodeId, mapping.targetNodeId);
        return this;
    }

    public Long get(Long sourceId) {
        var targetId = mappings.get(sourceId);
        if (targetId == null) {
            throw new IllegalStateException("Unable to find source node with id " + sourceId);
        }
        return targetId;
    }
}
