package org.neo4j.dbcopy.schema;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * The inferred shape of a graph: one entry per distinct node label set, one per relationship type.
 */
public record GraphSchema(List<NodeTypeSchema> nodeTypes, List<RelationshipTypeSchema> relTypes) {

    public GraphSchema {
        nodeTypes = List.copyOf(nodeTypes);
        relTypes = List.copyOf(relTypes);
    }

    public record NodeTypeSchema(SortedSet<String> labels, SortedMap<String, PropertyType> properties) {
    }

    public record RelationshipTypeSchema(String type, SortedMap<String, PropertyType> properties) {
    }
}
