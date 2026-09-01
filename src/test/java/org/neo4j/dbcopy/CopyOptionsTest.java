package org.neo4j.dbcopy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CopyOptionsTest {

    @Test
    void should_default_the_schema_sample_size_to_1000() {
        assertThat(CopyOptions.DEFAULT.schemaSampleSize()).isEqualTo(1000);
    }

    @Test
    void should_allow_overriding_the_schema_sample_size() {
        CopyOptions options = new CopyOptions.Builder().schemaSampleSize(50).build();

        assertThat(options.schemaSampleSize()).isEqualTo(50);
    }
}
