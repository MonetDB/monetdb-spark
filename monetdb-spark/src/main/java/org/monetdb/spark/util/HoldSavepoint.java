package org.monetdb.spark.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * A Savepoint that implements Autocloseable
 */
public class HoldSavepoint implements AutoCloseable {
    private final Connection conn;
    private final Savepoint savepoint;

    public HoldSavepoint(Connection conn) throws SQLException {
        this.conn = conn;
        if (conn.getAutoCommit()) {
            // No need for a Savepoint, just start a transaction
            conn.setAutoCommit(false);
            savepoint = null;
        } else {
            // autocommit was already off
            savepoint = conn.setSavepoint();
        }
    }

    @Override
    public void close() throws SQLException {
        if (savepoint == null) {
            conn.rollback();
            conn.setAutoCommit(true);
        } else {
            conn.rollback(savepoint);
        }
    }
}
