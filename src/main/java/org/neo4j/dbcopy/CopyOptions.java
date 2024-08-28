package org.neo4j.dbcopy;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CopyOptions {

    public static final CopyOptions DEFAULT = new CopyOptions.Builder().build();

    private final Set<String> nodePropertiesToExclude;
    private final Set<String> relationshipPropertiesToExclude;

    private CopyOptions(Builder builder) {
        this.nodePropertiesToExclude = builder.nodePropertiesToExclude;
        this.relationshipPropertiesToExclude = builder.relationshipPropertiesToExclude;
    }

    public Set<String> getNodePropertiesToExclude() {
        return Collections.unmodifiableSet(nodePropertiesToExclude);
    }

    public Set<String> getRelationshipPropertiesToExclude() {
        return Collections.unmodifiableSet(relationshipPropertiesToExclude);
    }

    public static class Builder {
        private Set<String> nodePropertiesToExclude = Collections.emptySet();
        private Set<String> relationshipPropertiesToExclude = Collections.emptySet();

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

        public CopyOptions build() {
            return new CopyOptions(this);
        }
    }
}
