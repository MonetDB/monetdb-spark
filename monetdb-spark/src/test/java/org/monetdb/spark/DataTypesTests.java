package org.monetdb.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.monetdb.spark.workerside.ConversionError;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

public class DataTypesTests {

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
	public void testDecimalx() {
		String type = "Decimal(8,3)";
		Column decs = col("id").cast(type);
		Column betterDecs = decs.plus(decs.divide(lit(10))).cast(type);
		testRoundTrip(10, betterDecs);
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18})
	public void testDecimal(int precision) {
		int scale = precision <= 3 ? precision - 1 : 3;
		Column idecs = col("id").cast("DECIMAL");
		Column decs = idecs.plus(idecs.divide(lit(9)));
		// Make them fit
		Column modulo = power(lit(10), lit(precision - scale)).cast(new DecimalType(precision - scale + 1, 0));
		Column reduced = round(decs, scale).mod(modulo).cast(new DecimalType(precision, scale));

		testRoundTrip(10, reduced);
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
		// Create the dataframe
		Dataset<Row> data = spark.range(n).withColumn("x", expr);

		// Add a NULL value
		DataType dataType = data.schema().fields()[1].dataType();
		Column nullExpr = lit(null).cast(dataType);
		Column condition = col("id").equalTo(2);
		Column newExpr = when(condition, nullExpr).otherwise(col("x"));
		data = data.withColumn("x", newExpr);

		testRoundTrip(data);
	}

	private void testRoundTrip(Dataset<Row> data) {
		// Create the table by Overwriting with an empty dataframe
		data
				.filter(lit(false))
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		// Append the data by Appending this dataframe
		data
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
				.sort("id");

		// Compare the contents
		Row[] origRows = data.collectAsList().toArray(new Row[0]);
		Row[] foundRows = found.collectAsList().toArray(new Row[0]);
		assertArrayEquals(origRows, foundRows);
	}

}