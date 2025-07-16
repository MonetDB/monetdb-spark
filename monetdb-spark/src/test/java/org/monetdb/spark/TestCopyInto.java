package org.monetdb.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.junit.jupiter.api.*;
import org.monetdb.spark.source.ConversionError;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Spark itself, to get to know it
 */
public class TestCopyInto {

	private final int N = 100_000;
	private final String TABLE = "foo";
	private final String OTHER_TABLE = "bar";

	@AutoClose
	private Connection conn;
	@AutoClose
	private Statement stmt;

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
		return spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("f", col("id").cast("Double").divide(2.0))
				.withColumn("t", concat(lit("x"), col("id")));
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
				.option("numPartitions", 1) // ignored!
				.save();

		// Verify the result
		verifyTestData();
	}

	@Test
	public void testSchemaMismatch() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();


		// Create dummy table with the wrong schema: f BIGINT instead of DOUBLE
		stmt.execute("DROP TABLE IF EXISTS " + OTHER_TABLE);
		String sql = "CREATE TABLE " + OTHER_TABLE + "(id BIGINT, b BOOLEAN, f BIGINT, t TEXT)";
		stmt.execute(sql);

		// Try to insert the data
		Dataset<Row> df = createTestData();
		try {
			df
					.write()
					.format("org.monetdb.spark")
					.mode(SaveMode.Append)
					.option("url", Config.databaseUrl())
					.option("dbtable", OTHER_TABLE)
					.save();
			fail("df.write() should have failed because f has the wrong type");
		} catch (RuntimeException e) {
			if (!(e.getCause() instanceof ConversionError))
				throw e;
		}

		stmt.execute("DROP TABLE " + OTHER_TABLE);
	}

	@Test
	public void testBooleanType() {
		testRoundTrip(10, col("id").mod(2).equalTo(0));
	}

	@Test
	public void testByteType() {
		testRoundTrip(10, "Byte");
	}

	@Disabled // it maps ShortType to INTEGER, then everything falls apart
	@Test
	public void testShortType() {
		testRoundTrip(10, "Short");
	}

	@Test
	public void testIntegerType() {
		testRoundTrip(10, "Integer");
	}

	@Test
	public void testLongType() {
		testRoundTrip(10, "Long");
	}

	@Test
	public void testFloatType() {
		testRoundTrip(10, col("id").cast("Float").divide(2.0));
	}

	@Test
	public void testDoubleType() {
		testRoundTrip(10, col("id").cast("Double").divide(2.0));
	}

	@Test
	public void testStringType() {
		testRoundTrip(10, concat(lit("x"), col("id")));
	}

	private void testRoundTrip(int n, String typeName) {
		Column expr = col("id").cast(typeName);
		testRoundTrip(n, expr);
	}

	private void testRoundTrip(int n, Column expr) {
		Dataset<Row> orig = spark.range(n).withColumn("x", expr);
		testRoundTrip(orig);
	}

	private void testRoundTrip(Dataset<Row> orig) {
		// Create the table
		orig
				.filter(lit(false))
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		// Append the data
		orig
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		Dataset<Row> found = spark
				.read()
				.format("jdbc")
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.load()
				.sort("id")
				;

		// Compare the contents
		Row[] origRows = orig.collectAsList().toArray(new Row[0]);
		Row[] foundRows = found.collectAsList().toArray(new Row[0]);
		assertArrayEquals(origRows, foundRows);
	}

}