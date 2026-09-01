package org.neo4j.dbcopy.io;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class NodeRecord implements Node {

    private final long id;
    private final List<String> labels;
    private final Map<String, Value> properties;

    public NodeRecord(long id, List<String> labels, Map<String, Value> properties) {
        this.id = id;
        this.labels = List.copyOf(labels);
        this.properties = Map.copyOf(properties);
    }

    @Override
    public String elementId() { // we don't care, this is unused
        return id + "";
    }

    @Override
    public long id() {
        return id;
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
    public boolean hasLabel(String label) {
        return labels.contains(label);
    }

    @Override
    public Iterable<String> labels() {
        return labels;
    }
}
