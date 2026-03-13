package org.monetdb.spark.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * A Savepoint that implements Autocloseable
 */
public class WithSavepoint implements AutoCloseable {
    private final Connection conn;
    private final Savepoint savepoint;
    private boolean commitOnClose = false;

    public WithSavepoint(Connection conn) throws SQLException {
        this.conn = conn;
        if (conn.getAutoCommit()) {
            // No need for a Savepoint, just start a transaction
            conn.setAutoCommit(false);
            savepoint = null;
        } else {
            // autocommit was already off, we're in a transaction so we need a savepoint
            savepoint = conn.setSavepoint();
        }
    }

    @Override
    public void close() throws SQLException {
        if (savepoint == null) {
            if (commitOnClose)
                conn.commit();
            else
                conn.rollback();
            conn.setAutoCommit(true);
        } else {
            if (commitOnClose)
                conn.releaseSavepoint(savepoint);
            else
                conn.rollback(savepoint);
        }
    }

    public boolean isCommitOnClose() {
        return commitOnClose;
    }

    public WithSavepoint setCommitOnClose(boolean commitOnClose) {
        this.commitOnClose = commitOnClose;
        return this;
    }
}
