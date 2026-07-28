package org.neo4j.dbcopy;

import org.neo4j.dbcopy.io.DataReader;
import org.neo4j.dbcopy.io.DataReaderFactory;
import org.neo4j.dbcopy.io.DataWriter;
import org.neo4j.dbcopy.io.DataWriterFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(
        name = "neo4j-db-copy",
        mixinStandardHelpOptions = true,
        version = "checksum 4.0",
        description = """
                Copy the content of a Neo4j database to another Neo4j database, or to/from a JSONL file.
                If the source/target address starts with bolt/neo4j, Bolt is used.
                If it starts with file, a JSONL file is used."""
)
class DbCopyCli implements Callable<Integer> {

    private final DataReaderFactory dataReaderFactory = new DataReaderFactory();
    private final DataWriterFactory dataWriterFactory = new DataWriterFactory();

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-sa", "--source-address"},
            required = true,
            description = "The source address: bolt/neo4j URI for Neo4j, or file:///path/to/file.jsonl for JSONL"
    )
    private URI sourceAddress;

    @Option(
            names = {"-su", "--source-username"},
            description = "The source database username to connect as (default: neo4j, used only for bolt/neo4j URIs)"
    )
    private String sourceUserName = "neo4j";

    @Option(
            names = {"-sp", "--source-password"},
            interactive = true,
            description = "The source database password to connect with (used only for bolt/neo4j URIs)"
    )
    private String sourcePassword;

    @Option(
            names = {"-sd", "--source-database"},
            description = "The source database to connect to (used only for bolt/neo4j URIs)."
    )
    private String sourceDatabase;

    @Option(
            names = {"-ta", "--target-address"},
            required = true,
            description = "The target address: bolt/neo4j URI for Neo4j, or file:///path/to/file.jsonl for JSONL"
    )
    private URI targetAddress;

    @Option(
            names = {"-tu", "--target-username"},
            description = "The target database username to connect as (default: neo4j, used only for bolt/neo4j URIs)"
    )
    private String targetUserName = "neo4j";

    @Option(
            names = {"-tp", "--target-password"},
            interactive = true,
            description = "The target database password to connect with (used only for bolt/neo4j URIs)"
    )
    private String targetPassword;

    @Option(
            names = {"-td", "--target-database"},
            description = "The target database to connect to (used only for bolt/neo4j URIs)."
    )
    private String targetDatabase;

    @Option(
            names = {"-enp", "--exclude-node-properties"},
            split = ",",
            description = "Comma-separated list of node properties to exclude from the copy"
    )
    private Set<String> excludeNodeProperties = new HashSet<>();

    @Option(
            names = {"-erp", "--exclude-relationship-properties"},
            split = ",",
            description = "Comma-separated list of relationship properties to exclude from the copy"
    )
    private Set<String> excludeRelationshipProperties = new HashSet<>();

    @Option(
            names = {"-lock", "--lock-source-database"},
            description = "Set the source database to read-only mode before copying (only valid for bolt/neo4j source)"
    )
    private boolean lockSourceDatabase = false;

    @Override
    public Integer call() {
        DataReader reader = null;
        DataWriter writer = null;
        DatabaseStateManager databaseStateManager;

        try {
            CopyOptions copyOptions = new CopyOptions.Builder()
                    .excludeNodeProperties(excludeNodeProperties)
                    .excludeRelationshipProperties(excludeRelationshipProperties)
                    .build();

            reader = dataReaderFactory.createReader(sourceAddress, sourceUserName, sourcePassword, sourceDatabase);
            databaseStateManager = reader.createStateManager(lockSourceDatabase);
            databaseStateManager.makeReadOnly();
            writer = dataWriterFactory.createWriter(targetAddress, targetUserName, targetPassword, targetDatabase);

            try {
                new DataTransfer(reader, writer, copyOptions)
                        .copyAllNodesAndRels()
                        .block();
            } finally {
                databaseStateManager.restoreInitialState();
            }

        } catch (CommandLine.ParameterException | IllegalArgumentException e) {
            spec.commandLine().getErr().println(e.getMessage());
            return 1;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        } finally {
            closeQuietly(writer);
            closeQuietly(reader);
        }

        return 0;
    }

    private void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            spec.commandLine().getErr().println("Failed to close resource: " + e.getMessage());
        }
    }

    public static void main(String... args) {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        int exitCode = new CommandLine(new DbCopyCli()).execute(args);
        System.exit(exitCode);
    }
}
