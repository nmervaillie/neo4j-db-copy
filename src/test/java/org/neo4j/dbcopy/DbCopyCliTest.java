package org.neo4j.dbcopy;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DbCopyCliTest {

    @Test
    void should_allow_json_endpoints_without_bolt_only_options() throws Exception {
        Path sourceFile = Files.createTempFile("dbcopy-source", ".jsonl");
        Path targetDir = Files.createTempDirectory("dbcopy-target-dir");
        Path targetFile = targetDir.resolve("export.jsonl");

        CommandResult result = execute(
                "-sa", sourceFile.toUri().toString(),
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isZero();
        assertThat(Files.exists(targetFile)).isTrue();
    }

    @Test
    void should_reject_existing_json_target_file() throws Exception {
        Path sourceFile = Files.createTempFile("dbcopy-source", ".jsonl");
        Path targetFile = Files.createTempFile("dbcopy-target", ".jsonl");

        CommandResult result = execute(
                "-sa", sourceFile.toUri().toString(),
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("Refusing to overwrite existing JSON export file");
    }

    @Test
    void should_require_source_bolt_options_for_bolt_source() throws Exception {
        Path targetFile = Files.createTempFile("dbcopy-target", ".jsonl");

        CommandResult result = execute(
                "-sa", "neo4j://localhost:7687",
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("--source-password is required");
    }

    @Test
    void should_require_target_bolt_options_for_bolt_target() throws Exception {
        Path sourceFile = Files.createTempFile("dbcopy-source", ".jsonl");

        CommandResult result = execute(
                "-sa", sourceFile.toUri().toString(),
                "-ta", "neo4j://localhost:7687"
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("--target-password is required");
    }

    @Test
    void should_reject_unsupported_schemes() throws Exception {
        Path targetFile = Files.createTempFile("dbcopy-target", ".jsonl");

        CommandResult result = execute(
                "-sa", "http://example.com/data.jsonl",
                "-ta", targetFile.toUri().toString()
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("Unsupported scheme 'http'");
    }

    @Test
    void should_reject_locking_for_json_source() throws Exception {
        Path sourceFile = Files.createTempFile("dbcopy-source", ".jsonl");
        Path targetFile = Files.createTempFile("dbcopy-target", ".jsonl");

        CommandResult result = execute(
                "-sa", sourceFile.toUri().toString(),
                "-ta", targetFile.toUri().toString(),
                "-lock"
        );

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.err).contains("--lock-source-database is only valid");
    }

    private CommandResult execute(String... args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine commandLine = new CommandLine(new DbCopyCli());
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));
        int exitCode = commandLine.execute(args);
        return new CommandResult(exitCode, out.toString(), err.toString());
    }

    private record CommandResult(int exitCode, String out, String err) {
    }
}
