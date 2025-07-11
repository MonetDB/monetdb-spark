package org.monetdb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Spark itself, to get to know it
 */
public class TestSpark {
	@AutoClose
	Connection conn = null;
	@AutoClose
	Statement stmt = null;
	@AutoClose
	SparkSession spark = null;

	@BeforeAll
	public static void checkMonetDBReachable() throws SQLException {
		Config.checkDatabase();
	}

	public void connect() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();
	}

	void spark() {
		spark = Config.sparkSession();
	}

	@Test
	public void testRoundtrip() throws SQLException {
		int n = 10;
		connect();
		stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo(id INT, b BOOLEAN, t TEXT)");

		spark();
		Dataset<Long> spine = spark.range(n);
		Dataset<Row> df = spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("t", concat(lit("x"), col("id")));

		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.option("truncate", true)
				.save();

		ResultSet rs = stmt.executeQuery("SELECT id, b, t FROM foo ORDER BY id");
		for (int i = 0; i < n; i++) {
			assertTrue(rs.next());
			assertEquals(i, rs.getInt(1));
			assertEquals(i % 2 == 0, rs.getBoolean(2));
			assertEquals("x" + i, rs.getString(3));
		}
		assertFalse(rs.next());
	}

	@Test
	public void testOverwrite() throws SQLException {
		int n = 10;
		connect();
		stmt.execute("DROP TABLE IF EXISTS foo");

		spark();

		Dataset<Long> spine = spark.range(n);
		Dataset<Row> df = spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("t", concat(lit("x"), col("id")));

		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.option("truncate", true)
				.option("numPartitions", 1)
				.save();

		ResultSet rs = stmt.executeQuery("SELECT id, b, t FROM foo ORDER BY id");
		for (int i = 0; i < n; i++) {
			assertTrue(rs.next());
			assertEquals(i, rs.getInt(1));
			assertEquals(i % 2 == 0, rs.getBoolean(2));
			assertEquals("x" + i, rs.getString(3));
		}
		assertFalse(rs.next());
	}
}