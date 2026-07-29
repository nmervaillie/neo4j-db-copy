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
        usageHelpWidth = 150,
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
            defaultValue = "${SOURCE_ADDRESS}",
            description = {
                    "The source address: bolt/neo4j URI for Neo4j, or",
                    "file:///path/to/file.jsonl for JSONL (env: SOURCE_ADDRESS)"
            }
    )
    private URI sourceAddress;

    @Option(
            names = {"-su", "--source-username"},
            defaultValue = "${SOURCE_USERNAME}",
            description = "The source database username to connect as (default: neo4j, used only for bolt/neo4j URIs) (env: SOURCE_USERNAME)"
    )
    private String sourceUserName = "neo4j";

    @Option(
            names = {"-sp", "--source-password"},
            interactive = true,
            defaultValue = "${SOURCE_PASSWORD}",
            description = "The source database password to connect with (used only for bolt/neo4j URIs) (env: SOURCE_PASSWORD)"
    )
    private String sourcePassword;

    @Option(
            names = {"-sd", "--source-database"},
            defaultValue = "${SOURCE_DATABASE}",
            description = "The source database to connect to (used only for bolt/neo4j URIs) (env: SOURCE_DATABASE)."
    )
    private String sourceDatabase;

    @Option(
            names = {"-ta", "--target-address"},
            required = true,
            defaultValue = "${TARGET_ADDRESS}",
            description = {
                    "The target address: bolt/neo4j URI for Neo4j, or",
                    "file:///path/to/file.jsonl for JSONL (env: TARGET_ADDRESS)"
            }
    )
    private URI targetAddress;

    @Option(
            names = {"-tu", "--target-username"},
            defaultValue = "${TARGET_USERNAME}",
            description = "The target database username to connect as (default: neo4j, used only for bolt/neo4j URIs) (env: TARGET_USERNAME)"
    )
    private String targetUserName = "neo4j";

    @Option(
            names = {"-tp", "--target-password"},
            interactive = true,
            defaultValue = "${TARGET_PASSWORD}",
            description = "The target database password to connect with (used only for bolt/neo4j URIs) (env: TARGET_PASSWORD)"
    )
    private String targetPassword;

    @Option(
            names = {"-td", "--target-database"},
            defaultValue = "${TARGET_DATABASE}",
            description = "The target database to connect to (used only for bolt/neo4j URIs) (env: TARGET_DATABASE)."
    )
    private String targetDatabase;

    @Option(
            names = "--create-target-database",
            defaultValue = "${CREATE_TARGET_DATABASE}",
            description = "Create the target database if it does not exist (only valid for bolt/neo4j targets) (env: CREATE_TARGET_DATABASE)"
    )
    private boolean createTargetDatabase = false;

    @Option(
            names = {"-enp", "--exclude-node-properties"},
            split = ",",
            defaultValue = "${EXCLUDE_NODE_PROPERTIES}",
            description = "Comma-separated list of node properties to exclude from the copy (env: EXCLUDE_NODE_PROPERTIES)"
    )
    private Set<String> excludeNodeProperties = new HashSet<>();

    @Option(
            names = {"-erp", "--exclude-relationship-properties"},
            split = ",",
            defaultValue = "${EXCLUDE_RELATIONSHIP_PROPERTIES}",
            description = "Comma-separated list of relationship properties to exclude from the copy (env: EXCLUDE_RELATIONSHIP_PROPERTIES)"
    )
    private Set<String> excludeRelationshipProperties = new HashSet<>();

    @Option(
            names = {"-lock", "--lock-source-database"},
            defaultValue = "${LOCK_SOURCE_DATABASE}",
            description = "Set the source database to read-only mode before copying (only valid for bolt/neo4j source) (env: LOCK_SOURCE_DATABASE)"
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
            writer = dataWriterFactory.createWriter(targetAddress, targetUserName, targetPassword, targetDatabase, createTargetDatabase);

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
