package org.neo4j.dbcopy;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CopyOptions {

    public static final CopyOptions DEFAULT = new CopyOptions.Builder().build();

    private final Set<String> nodePropertiesToExclude;
    private final Set<String> relationshipPropertiesToExclude;
    private final int batchSize;
    private final int schemaSampleSize;

    private CopyOptions(Builder builder) {
        this.nodePropertiesToExclude = builder.nodePropertiesToExclude;
        this.relationshipPropertiesToExclude = builder.relationshipPropertiesToExclude;
        this.batchSize = builder.batchSize;
        this.schemaSampleSize = builder.schemaSampleSize;
    }

    public Set<String> getNodePropertiesToExclude() {
        return Collections.unmodifiableSet(nodePropertiesToExclude);
    }

    public Set<String> getRelationshipPropertiesToExclude() {
        return Collections.unmodifiableSet(relationshipPropertiesToExclude);
    }

    public int batchSize() {
        return batchSize;
    }

    /**
     * How many rows per node label set and per relationship type the parquet schema scan samples.
     * Not exposed as a CLI option yet.
     */
    public int schemaSampleSize() {
        return schemaSampleSize;
    }

    public static class Builder {
        private Set<String> nodePropertiesToExclude = Collections.emptySet();
        private Set<String> relationshipPropertiesToExclude = Collections.emptySet();
        private int batchSize = 5000;
        private int schemaSampleSize = 1000;

        public Builder excludeNodeProperties(Set<String> properties) {
            Objects.requireNonNull(properties);
            this.nodePropertiesToExclude = properties;
            return this;
        }

        public Builder excludeRelationshipProperties(Set<String> properties) {
            Objects.requireNonNull(properties);
            this.relationshipPropertiesToExclude = properties;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder schemaSampleSize(int schemaSampleSize) {
            this.schemaSampleSize = schemaSampleSize;
            return this;
        }

        public CopyOptions build() {
            return new CopyOptions(this);
        }
    }
}
