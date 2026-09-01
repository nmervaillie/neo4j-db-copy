package org.neo4j.dbcopy.bolt;

import org.neo4j.dbcopy.CopyOptions;
import org.neo4j.dbcopy.schema.GraphSchema;
import org.neo4j.dbcopy.schema.GraphSchema.NodeTypeSchema;
import org.neo4j.dbcopy.schema.GraphSchema.RelationshipTypeSchema;
import org.neo4j.dbcopy.schema.Neo4jTypeInference;
import org.neo4j.dbcopy.schema.PropertyType;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.MapAccessor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.neo4j.driver.Values.parameters;

/**
 * Infers a {@link GraphSchema} by sampling a bounded number of rows per node label set and per
 * relationship type. Label sets and relationship types are discovered exhaustively; only the
 * property types are sampled, which is why a non-normalized graph can still surprise the writer.
 */
public final class BoltSchemaScanner {

    private static final String WARNING = """
            Parquet export is experimental. The graph schema is inferred by sampling %d nodes per \
            label set and %d relationships per type. The export will fail if property types are not \
            consistent (for example 'age' is an Integer on some :Person nodes and a String on \
            others), or if a property is absent from every sampled row.""";

    private final Driver driver;
    private final String database;

    public BoltSchemaScanner(Driver driver, String database) {
        this.driver = driver;
        this.database = database;
    }

    public GraphSchema scan(CopyOptions copyOptions) {
        int sampleSize = copyOptions.schemaSampleSize();
        System.err.println(WARNING.formatted(sampleSize, sampleSize));

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            List<NodeTypeSchema> nodeTypes = scanNodeTypes(session, sampleSize, copyOptions);
            List<RelationshipTypeSchema> relTypes = scanRelationshipTypes(session, sampleSize, copyOptions);
            return new GraphSchema(nodeTypes, relTypes);
        }
    }

    private List<NodeTypeSchema> scanNodeTypes(Session session, int sampleSize,
                                               CopyOptions copyOptions) {
        List<NodeTypeSchema> nodeTypes = new ArrayList<>();
        for (SortedSet<String> labels : discoverLabelSets(session)) {
            SortedMap<String, PropertyType> properties = sampleNodeProperties(
                    session, labels, sampleSize, copyOptions.getNodePropertiesToExclude());
            if (properties != null) {
                nodeTypes.add(new NodeTypeSchema(labels, properties));
            }
        }
        return nodeTypes;
    }

    /**
     * The empty label set is added explicitly as a fallback, but {@code db.schema.nodeTypeProperties()}
     * may also report it itself (for unlabelled nodes, with {@code nodeLabels = []}), so results are
     * deduplicated by their normalized (sorted) label set.
     */
    private List<SortedSet<String>> discoverLabelSets(Session session) {
        Set<SortedSet<String>> labelSets = new LinkedHashSet<>();
        labelSets.add(new TreeSet<>());
        session.run("""
                        CALL db.schema.nodeTypeProperties() YIELD nodeLabels
                        WITH DISTINCT nodeLabels RETURN nodeLabels""")
                .list()
                .forEach(record -> labelSets.add(
                        new TreeSet<>(record.get("nodeLabels").asList(Value::asString))));
        return new ArrayList<>(labelSets);
    }

    /**
     * @return the sampled property types, or {@code null} when the label set has no nodes at all.
     */
    private SortedMap<String, PropertyType> sampleNodeProperties(Session session,
                                                                 SortedSet<String> labels,
                                                                 int sampleSize,
                                                                 Set<String> excluded) {
        String query = "MATCH (n" + labelPredicate(labels) + ") WHERE size(labels(n)) = $labelCount "
                + "WITH n LIMIT $sample RETURN n";
        List<Record> records = session.run(query,
                parameters("labelCount", labels.size(), "sample", sampleSize)).list();
        if (records.isEmpty()) {
            return null;
        }
        return mergeProperties(records, "n", excluded, describe(labels));
    }

    private List<RelationshipTypeSchema> scanRelationshipTypes(Session session, int sampleSize,
                                                               CopyOptions copyOptions) {
        List<RelationshipTypeSchema> relTypes = new ArrayList<>();
        List<String> types = session
                .run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                .list(record -> record.get("relationshipType").asString());

        for (String type : types) {
            String query = "MATCH ()-[r:" + quote(type) + "]->() WITH r LIMIT $sample RETURN r";
            List<Record> records = session.run(query, parameters("sample", sampleSize)).list();
            if (records.isEmpty()) {
                continue;
            }
            relTypes.add(new RelationshipTypeSchema(type, mergeProperties(records, "r",
                    copyOptions.getRelationshipPropertiesToExclude(), "relationship type " + type)));
        }
        return relTypes;
    }

    private static SortedMap<String, PropertyType> mergeProperties(List<Record> records,
                                                                   String column,
                                                                   Set<String> excluded,
                                                                   String context) {
        SortedMap<String, PropertyType> properties = new TreeMap<>();
        for (Record record : records) {
            Entity entity = record.get(column).asEntity();
            for (String key : entity.keys()) {
                if (excluded.contains(key)) {
                    continue;
                }
                PropertyType observed = inferOrFail(entity, key, context);
                properties.merge(key, observed, (existing, seen) -> mergeOrFail(existing, seen, key, context));
            }
        }
        return properties;
    }

    private static PropertyType inferOrFail(MapAccessor entity, String key, String context) {
        try {
            return Neo4jTypeInference.inferType(entity.get(key));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Schema scan failed on " + context + ", property '"
                    + key + "': " + e.getMessage(), e);
        }
    }

    private static PropertyType mergeOrFail(PropertyType existing, PropertyType observed,
                                            String key, String context) {
        try {
            return Neo4jTypeInference.merge(existing, observed);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Schema scan failed on " + context + ": property '"
                    + key + "' does not have a consistent type. " + e.getMessage()
                    + " Parquet export requires one type per property.", e);
        }
    }

    private static String labelPredicate(SortedSet<String> labels) {
        StringBuilder predicate = new StringBuilder();
        labels.forEach(label -> predicate.append(':').append(quote(label)));
        return predicate.toString();
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String describe(Set<String> labels) {
        return labels.isEmpty() ? "unlabelled nodes" : "label set :" + String.join(":", labels);
    }
}
