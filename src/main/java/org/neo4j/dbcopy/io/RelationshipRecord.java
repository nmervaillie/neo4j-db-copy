package org.neo4j.dbcopy.io;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record RelationshipRecord(long id, long startId, long endId, String type,
                                 Map<String, Value> properties) implements Relationship {

    public RelationshipRecord(long id,
                              long startId,
                              long endId,
                              String type,
                              Map<String, Value> properties) {
        this.id = id;
        this.startId = startId;
        this.endId = endId;
        this.type = type;
        this.properties = Map.copyOf(properties);
    }

    @Override
    public String elementId() {
        return id + "";
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Value> e : properties.entrySet()) {
            result.put(e.getKey(), e.getValue().asObject());
        }
        return result;
    }

    @Override
    public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {
        Map<String, T> result = new LinkedHashMap<>();
        for (Map.Entry<String, Value> e : properties.entrySet()) {
            result.put(e.getKey(), mapFunction.apply(e.getValue()));
        }
        return result;
    }

    @Override
    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    @Override
    public Value get(String key) {
        return properties.getOrDefault(key, Values.NULL);
    }

    @Override
    public Iterable<String> keys() {
        return properties.keySet();
    }

    @Override
    public int size() {
        return properties.size();
    }

    @Override
    public Iterable<Value> values() {
        return properties.values();
    }

    @Override
    public <T> Iterable<T> values(Function<Value, T> mapFunction) {
        List<T> result = new ArrayList<>(properties.size());
        for (Value v : properties.values()) {
            result.add(mapFunction.apply(v));
        }
        return result;
    }

    @Override
    public boolean hasType(String relationshipType) {
        return type.equals(relationshipType);
    }

    @Override
    public long startNodeId() {
        return startId;
    }

    @Override
    public String startNodeElementId() {
        return startId + "";
    }

    @Override
    public long endNodeId() {
        return endId;
    }

    @Override
    public String endNodeElementId() {
        return endId + "";
    }
}
