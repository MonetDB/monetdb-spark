package org.monetdb.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.junit.jupiter.api.*;
import org.monetdb.spark.util.MyAutoClose;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SparkSQLTests {
    @MyAutoClose
    Connection conn;
    @MyAutoClose
    Statement stmt;
    String sparkDbUrl;
    @MyAutoClose
    private SparkSession spark;

    @BeforeEach
    public void setUp() throws SQLException {
        String testDbUrl = Config.databaseUrl("client_remark=connection%20from%20test%20class");
        sparkDbUrl = Config.databaseUrl("client_remark=connection%20from%20Spark");
        conn = DriverManager.getConnection(testDbUrl);
        stmt = conn.createStatement();
    }

    public void startSpark() {
        startSpark(Map.of());
    }

    public void startSpark(Map<String,Object> options) {
        assertNull(spark);
        spark = SparkSession.builder()
                .appName("unit_tests")
                .config("spark.master", "local[1]")
                .config(Map.of(
                        "spark.sql.catalog.testdb", JDBCTableCatalog.class.getName(),
                        "spark.sql.catalog.testdb.url", Config.databaseUrl()
                ))
                .config(options)
                .getOrCreate();
    }

    public void createTables() throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS foo");
        stmt.execute("CREATE TABLE foo(i INT, t TEXT)");
        stmt.execute("INSERT INTO foo VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    }

    @Test
    public void testSelectStar() throws SQLException {
        startSpark();
        createTables();
        List<Row> x = spark.sql("SELECT * FROM testdb.foo").collectAsList();
        assertEquals(3, x.size());
    }

    @Test
    public void testSelectWhere() throws SQLException {
        startSpark();
        createTables();
        // We hope the WHERE is pushed to Monet but there's no easy way to check this
        List<Row> x = spark.sql("SELECT * FROM testdb.foo WHERE i = 1").collectAsList();
        assertEquals(1, x.size());
    }

    @Test
    public void testSelectLimitOffset() throws SQLException {
        startSpark();
        createTables();
        List<Row> x = spark.sql("SELECT * FROM testdb.foo ORDER BY i LIMIT 1 OFFSET 1").collectAsList();
        assertEquals(1, x.size());
        Row row0 = x.get(0);
        // Huh!!?? row.getInt is 0-based!
        assertEquals(2, row0.getInt(0));
        assertEquals("two", row0.getString(1));
    }


}
