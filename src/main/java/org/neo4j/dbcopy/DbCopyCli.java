package org.neo4j.dbcopy;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(name = "neo4j-db-copy", mixinStandardHelpOptions = true, version = "checksum 4.0",
         description = "Copy the content of a Neo4j database to another Neo4j database, via the network, through the bolt protocol.")
class DbCopyCli implements Callable<Integer> {

    @Option(names = {"-sa", "--source-address"}, required = true, description = "The source database address (ex: neo4j+s://my-server:7687)")
    private URI sourceAddress;

    @Option(names = {"-su", "--source-username"}, description = "The source database username to connect as (default: neo4j)")
    private String sourceUserName = "neo4j";

    @Option(names = {"-sp", "--source-password"}, required = true, interactive = true, description = "The source database password to connect with")
    private String sourcePassword;

    @Option(names = {"-sd", "--source-database"}, required = true, description = "The source database to connect to.")
    private String sourceDatabase;

    @Option(names = {"-ta", "--target-address"}, required = true, description = "The target database address (ex: neo4j+s://my-server:7687)")
    private URI targetAddress;

    @Option(names = {"-tu", "--target-username"}, description = "The target database username to connect as (default: neo4j)")
    private String targetUserName = "neo4j";

    @Option(names = {"-tp", "--target-password"}, required = true, interactive = true, description = "The target database password to connect with")
    private String targetPassword;

    @Option(names = {"-td", "--target-database"}, required = true, description = "The target database to connect to.")
    private String targetDatabase;

    @Option(names = {"-enp", "--exclude-node-properties"}, split = ",", description = "Comma-separated list of node properties to exclude from the copy")
    private Set<String> excludeNodeProperties = new HashSet<>();

    @Option(names = {"-erp", "--exclude-relationship-properties"}, split = ",", description = "Comma-separated list of relationship properties to exclude from the copy")
    private Set<String> excludeRelationshipProperties = new HashSet<>();

    @Option(names = {"-lock", "--lock-source-database"}, description = "Set the source database to read-only mode before copying")
    private boolean lockSourceDatabase = false;

    @Override
    public Integer call() {

        try (Driver sourceDriver = GraphDatabase.driver(sourceAddress, AuthTokens.basic(sourceUserName, sourcePassword));
             Driver targetDriver = GraphDatabase.driver(targetAddress, AuthTokens.basic(targetUserName, targetPassword))) {
             sourceDriver.verifyConnectivity();
             targetDriver.verifyConnectivity();

            CopyOptions copyOptions = new CopyOptions.Builder()
                    .excludeNodeProperties(excludeNodeProperties)
                    .excludeRelationshipProperties(excludeRelationshipProperties)
                    .build();

            DatabaseStateManager databaseStateManager = (lockSourceDatabase) ? new LockingDatabaseStateManager(sourceDriver, sourceDatabase) : new DatabaseStateManager(){};
            databaseStateManager.makeReadOnly();
            try {
                new DataTransfer(sourceDriver, sourceDatabase, targetDriver, targetDatabase, copyOptions).copyAllNodesAndRels().block();
            } finally {
                databaseStateManager.restoreInitialState();
            }
        }
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new DbCopyCli()).execute(args);
        System.exit(exitCode);
    }
}