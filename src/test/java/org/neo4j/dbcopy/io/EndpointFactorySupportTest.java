package org.neo4j.dbcopy.io;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndpointFactorySupportTest {

    private final TestEndpointFactorySupport support = new TestEndpointFactorySupport();

    @Test
    void should_detect_bolt_schemes() {
        assertThat(support.endpointKindFor(URI.create("bolt://localhost:7687")))
                .isEqualTo(EndpointFactorySupport.EndpointKind.BOLT);
        assertThat(support.endpointKindFor(URI.create("neo4j+s://localhost:7687")))
                .isEqualTo(EndpointFactorySupport.EndpointKind.BOLT);
    }

    @Test
    void should_detect_file_scheme_as_json() {
        assertThat(support.endpointKindFor(URI.create("file:///tmp/export.jsonl")))
                .isEqualTo(EndpointFactorySupport.EndpointKind.JSON);
    }

    @Test
    void should_reject_unsupported_schemes() {
        assertThatThrownBy(() -> support.endpointKindFor(URI.create("http://example.com/export.jsonl")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported scheme 'http'");
    }

    @Test
    void should_require_non_blank_bolt_option_values() {
        assertThatThrownBy(() -> support.requireOptionValue("--source-password", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--source-password is required");
    }

    @Test
    void should_convert_file_uri_to_path() {
        Path path = support.jsonFilePath(URI.create("file:///tmp/export.jsonl"), "source");

        assertThat(path).isEqualTo(Path.of("/tmp/export.jsonl"));
    }

    @Test
    void should_reject_non_file_json_uri() {
        assertThatThrownBy(() -> support.jsonFilePath(URI.create("neo4j://localhost:7687"), "source"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must use a file URI");
    }

    private static final class TestEndpointFactorySupport extends EndpointFactorySupport {

        EndpointKind endpointKindFor(URI uri) {
            return endpointKind(uri);
        }

        void requireOptionValue(String optionName, String value) {
            requireOption(optionName, value);
        }

        Path jsonFilePath(URI uri, String endpointName) {
            return toJsonFilePath(uri, endpointName);
        }
    }
}
