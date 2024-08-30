package org.neo4j.dbcopy;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LockingDatabaseStateManager implements DatabaseStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(LockingDatabaseStateManager.class);

    private final Driver driver;
    private final String dbName;
    private boolean wasInReadWriteState = false;

    public LockingDatabaseStateManager(Driver driver, String dbName) {
        this.driver = driver;
        this.dbName = dbName;
    }

    record DatabaseInfo(String name, String status, String access) {

        public boolean isOnline() {
            return status.equals("online");
        }

        public boolean isReadWrite() {
            return access.equals("read-write");
        }
    }

    @Override
    public void makeReadOnly() {
        var dbInfo = getDatabaseInfo();
        if (!dbInfo.isOnline()) {
            throw new IllegalStateException("Unable to proceed. Database " + dbName + " is " + dbInfo.status);
        }
        if (dbInfo.isReadWrite()) {
            setDatabaseAccessMode("READ ONLY");
            wasInReadWriteState = true;
        }
    }

    @Override
    public void restoreInitialState() {
        if (wasInReadWriteState) {
            setDatabaseAccessMode("READ WRITE");
        }
    }

    DatabaseInfo getDatabaseInfo() {
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            Record result = session.run("SHOW DATABASE " + dbName).single();
            return new DatabaseInfo(dbName, result.get("currentStatus").asString(), result.get("access").asString());
        } catch (NoSuchRecordException e) {
            throw new IllegalStateException("Unable to find database " + dbName);
        }
    }

    void setDatabaseAccessMode(String accessMode) {
        LOG.info("Setting Database {} to {} mode", dbName, accessMode);
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("ALTER DATABASE " + dbName + " SET ACCESS " + accessMode + " WAIT");
            });
        }
    }
}
