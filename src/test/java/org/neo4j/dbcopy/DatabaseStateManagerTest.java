package org.neo4j.dbcopy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class DatabaseStateManagerTest {

    private LockingDatabaseStateManager databaseStateManager;

    @BeforeEach
    void setUp() {
        var mockDriver = mock(Driver.class);
        var mockSession = mock(Session.class);
        when(mockDriver.session(any(SessionConfig.class))).thenReturn(mockSession);

        databaseStateManager = spy(new LockingDatabaseStateManager(mockDriver, "testdb"));
    }

    @Test
    void should_set_database_to_read_only_when_in_read_write() {
        LockingDatabaseStateManager.DatabaseInfo dbInfo = new LockingDatabaseStateManager.DatabaseInfo("testdb", "online", "read-write");
        doReturn(dbInfo).when(databaseStateManager).getDatabaseInfo();

        databaseStateManager.makeReadOnly();

        verify(databaseStateManager).setDatabaseAccessMode("READ ONLY");
    }

    @Test
    void should_not_set_database_to_read_only_when_already_read_only() {
        LockingDatabaseStateManager.DatabaseInfo dbInfo = new LockingDatabaseStateManager.DatabaseInfo("testdb", "online", "read-only");
        doReturn(dbInfo).when(databaseStateManager).getDatabaseInfo();

        databaseStateManager.makeReadOnly();

        verify(databaseStateManager, never()).setDatabaseAccessMode(anyString());
    }

    @Test
    void should_restore_database_to_read_write_if_it_was_changed() {
        LockingDatabaseStateManager.DatabaseInfo dbInfo = new LockingDatabaseStateManager.DatabaseInfo("testdb", "online", "read-write");
        doReturn(dbInfo).when(databaseStateManager).getDatabaseInfo();

        databaseStateManager.makeReadOnly();
        verify(databaseStateManager).setDatabaseAccessMode("READ ONLY");
        databaseStateManager.restoreInitialState();
        verify(databaseStateManager).setDatabaseAccessMode("READ WRITE");
    }

    @Test
    void should_not_restore_database_to_read_write_if_it_was_not_changed() {
        LockingDatabaseStateManager.DatabaseInfo dbInfo = new LockingDatabaseStateManager.DatabaseInfo("testdb", "online", "read-only");
        doReturn(dbInfo).when(databaseStateManager).getDatabaseInfo();

        databaseStateManager.makeReadOnly();
        databaseStateManager.restoreInitialState();

        verify(databaseStateManager, never()).setDatabaseAccessMode(anyString());
    }

    @Test
    void should_throw_exception_if_database_not_online() {
        LockingDatabaseStateManager.DatabaseInfo dbInfo = new LockingDatabaseStateManager.DatabaseInfo("testdb", "offline", "read-write");
        doReturn(dbInfo).when(databaseStateManager).getDatabaseInfo();

        assertThatThrownBy(() -> databaseStateManager.makeReadOnly())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to proceed. Database testdb is offline");
    }

    @Test
    void should_throw_exception_if_database_not_found() {
        doThrow(new IllegalStateException("Unable to find database testdb")).when(databaseStateManager).getDatabaseInfo();

        assertThatThrownBy(() -> databaseStateManager.makeReadOnly())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to find database testdb");
    }
}
