package org.monetdb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Spark itself, to get to know it
 */
public class TestCopyInto {

	private final int N = 5;
	private final String TABLE = "foo";

	@AutoClose
	private SparkSession spark;

	@BeforeAll
	public static void checkMonetDBReachable() throws SQLException {
		Config.checkDatabase();
	}

	@BeforeAll
	public static void startSpark() throws IOException {
		Config.sparkSession().close();
	}

	@BeforeEach
	public void startSparkSession() {
		spark = Config.sparkSession();
	}

	private Dataset<Row> createTestData() {
		Dataset<Long> spine = spark.range(N);
		Dataset<Row> df = spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("f", col("id").cast("Double").divide(2.0))
				.withColumn("t", concat(lit("x"), col("id")));
		return df;
	}

	private void verifyTestData() throws SQLException {
		String sql = "SELECT id, b, f, t FROM " + TABLE + " ORDER BY id";
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)
		) {
			int rowNr = 0;
			for (; rs.next(); rowNr++) {
				assertEquals(rowNr, rs.getInt("id"));
				assertEquals(rowNr % 2 == 0, rs.getBoolean("b"));
				assertEquals("x" + rowNr, rs.getString("t"));
				// Dividing an integer by two is exact on all relevant floating
				// point architectures.
				assertEquals((double) rowNr / 2.0, rs.getDouble("f"));
			}
			int rowCount = rowNr;
			assertEquals(N, rowCount);
		}
	}

	@Test
	public void testInsertSomeData() throws SQLException {
		// Create a nice data set
		Dataset<Row> df = createTestData();

		// Derive an empty dataframe and use the JDBC writer on it
		// to create the destination table
		Dataset<Row> empty = df.filter(lit(false));
		empty
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		// Now append to it with the CopyInto writer
		df
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.option("numPartitions", 1) // first test with one partition
				.save();

		// Verify the result
		verifyTestData();
	}


}