package org.monetdb.spark.playground;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * We can only easily test that it works, not that it actually gets compressed.
 */
public class TestBackRef {
    @AutoClose
    Connection conn;
    @AutoClose
    SparkSession spark;
    private final Map<String, String> extraOptions = new HashMap<>();

    @BeforeEach
    public void setUp() throws SQLException {
        conn = Config.connectDatabase();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS foo");
        }
        spark = Config.sparkSession();
    }

    private void set(String key, String value) {
        extraOptions.put(key, value);
    }

    @Test
    public void testRepeatingSingleCharacters() throws SQLException {
        runTest("A", "B", "A", "", null);
    }

    @Test
    public void testRepeatingMultiCharactersDefault() throws SQLException {
        runTest("foo", "bar", "", null);
    }

    @Test
    public void testRepeatingMultiCharactersEnabled() throws SQLException {
        set("backrefsize", "64k");
        runTest("foo", "bar", "", null);
    }

    @Test
    public void testSingleWithBatchSize() throws SQLException {
        set("batchsize", "50");
        runTest("A", "B", "A", "", null);
    }

    @Test
    public void testMultiWithBatchSize() throws SQLException {
        set("batchsize", "50");
        set("backrefsize", "64k");
        runTest("AAP", "NOOT", "AAP", "", null);
    }

    private void runTest(String... vocabulary) throws SQLException {
        int nrows = 100;
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < nrows; i++) {
            String s = vocabulary[i % vocabulary.length];
            rows.add(RowFactory.create(i, s));
        }
        var schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("s", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(rows, schema).coalesce(1);

        DataFrameWriter<Row> writer = df.write()
                .format("org.monetdb.spark")
                .mode(SaveMode.Overwrite)
                .option("url", Config.databaseUrl())
                .option("dbtable", "foo")
                .options(extraOptions);
        writer.save();

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT s FROM foo ORDER BY id")) {
                for (int i = 0; i < nrows; i++) {
                    assertTrue(rs.next(), "at row " + i);
                    String expected = vocabulary[i % vocabulary.length];
                    String actual = rs.getString("s");
                    assertEquals(expected, actual, "at row " + i);
                }
                assertFalse(rs.next());
            }
        }
    }


}
