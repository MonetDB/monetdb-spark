package org.monetdb.spark.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class WithSavepointTest {
    @MyAutoClose
    Connection conn;
    @MyAutoClose
    Statement stmt;
    @MyAutoClose
    PreparedStatement insertStmt;
    @MyAutoClose
    PreparedStatement lookupStmt;

    @BeforeEach
    public void setUp() throws SQLException {
        conn = Config.connectDatabase();
        stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS foo");
        stmt.execute("CREATE TABLE foo(i INT)");
        insertStmt = conn.prepareStatement("INSERT INTO foo VALUES (?)");
        lookupStmt = conn.prepareStatement("SELECT i FROM foo WHERE i = ? LIMIT 1");
    }

    private void insert(int i) throws SQLException {
        insertStmt.setInt(1, i);
        insertStmt.execute();
    }

    private boolean contains(int i) throws SQLException {
        lookupStmt.setInt(1, i);
        try (ResultSet rs = lookupStmt.executeQuery()) {
            return rs.next();
        }
    }

    @Test
    public void testDefault() throws SQLException {
        try (WithSavepoint sp = new WithSavepoint(conn)) {
            assertFalse(sp.isCommitOnClose());
        }
    }

    @Test
    public void testRollbackOutsideTransaction() throws SQLException {
        conn.setAutoCommit(true);
        insert(10);
        try (WithSavepoint sp = new WithSavepoint(conn)) {
            insert(30);
            assertTrue(contains(30));
        }
        assertFalse(contains(30));
        assertTrue(contains(10));
    }

    @Test
    public void testCommitOutsideTransaction() throws SQLException {
        conn.setAutoCommit(true);
        insert(10);
        try (WithSavepoint sp = new WithSavepoint(conn)) {
            insert(30);
            assertTrue(contains(30));
            sp.setCommitOnClose(true);
        }
        assertTrue(contains(30));
        assertTrue(contains(10));
    }

    @Test
    public void testRollbackInsideTransaction() throws SQLException {
        insert(10);
        conn.setAutoCommit(false);
        insert(20);
        try (WithSavepoint sp = new WithSavepoint(conn)) {
            insert(30);
            assertTrue(contains(30));
        }
        assertFalse(contains(30));
        assertTrue(contains(20));
        assertTrue(contains(10));

        conn.rollback();
        assertFalse(contains(30));
        assertFalse(contains(20));
        assertTrue(contains(10));
    }

    @Test
    public void testCommitInsideTransaction() throws SQLException {
        insert(10);
        conn.setAutoCommit(false);
        insert(20);
        try (WithSavepoint sp = new WithSavepoint(conn)) {
            insert(30);
            assertTrue(contains(30));
            sp.setCommitOnClose(true);
        }
        assertTrue(contains(30));
        assertTrue(contains(20));
        assertTrue(contains(10));

        conn.rollback();
        assertFalse(contains(30));
        assertFalse(contains(20));
        assertTrue(contains(10));
    }


}