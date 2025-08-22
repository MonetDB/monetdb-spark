package org.monetdb.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
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
	private int nrows;
	private String forceType;

	@BeforeAll
	public static void checkMonetDBReachable() throws SQLException {
		Config.checkDatabase();
	}

	@BeforeAll
	public static void startSpark() throws IOException {
		Config.sparkSession().close();
	}

	private static Column decimalTestData(int precision) {
		int scale = precision <= 3 ? precision - 1 : 3;
		Column idecs = col("id").cast("DECIMAL");
		Column decs = idecs.plus(idecs.divide(lit(9)));
		// Make them fit
		Column modulo = power(lit(10), lit(precision - scale)).cast(new DecimalType(precision - scale + 1, 0));
		Column reduced = round(decs, scale).mod(modulo).cast(new DecimalType(precision, scale));
		return reduced;
	}

	@BeforeEach
	public void setUp() {
		spark = Config.sparkSession();
		nrows = 10;
		forceType = null;
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
		testRoundTrip(col("id").mod(2).equalTo(0));
	}

	@Test
	public void testByteType() {
		forceType = "TINYINT"; // by default, it becomes SMALLINT
		testRoundTrip("Byte");
	}

	@Test
	public void testShortType() {
		forceType = "SMALLINT"; // by default, it becomes INT
		testRoundTrip("Short");
	}

	@Test
	public void testIntegerType() {
		forceType = "INTEGER"; // by default, it becomes BIGINT
		testRoundTrip("Integer");
	}

	@Test
	public void testLongType() {
		testRoundTrip("Long");
	}

	@Test
	public void testFloatType() {
		testRoundTrip(col("id").cast("Float").divide(2.0));
	}

	@Test
	public void testDoubleType() {
		testRoundTrip(col("id").cast("Double").divide(2.0));
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18})
	public void testDecimal(int precision) {
		Column data = decimalTestData(precision);
		testRoundTrip(data);
	}

	@ParameterizedTest
	@ValueSource(ints = {19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38})
	public void testHugeDecimal(int precision) {
		Column data = decimalTestData(precision);
		testRoundTrip(data);
	}

	@Test
	public void testStringType() {
		testRoundTrip(concat(lit("x"), col("id")));
	}

	@Test
	public void testDateType() {
		testRoundTrip(current_date().plus(col("id").cast("INTERVAL DAY")));
	}

	private void testRoundTrip(String typeName) {
		Column expr = col("id").cast(typeName);
		testRoundTrip(expr);
	}

	private void testRoundTrip(Column expr) {
		// Create the dataframe
		Dataset<Row> data = spark.range(-2, nrows - 2).withColumn("x", expr);

		// Add a NULL value
		DataType dataType = data.schema().fields()[1].dataType();
		Column nullExpr = lit(null).cast(dataType);
		Column condition = col("id").equalTo(2);
		Column newExpr = when(condition, nullExpr).otherwise(col("x"));
		data = data.withColumn("x", newExpr);

		testRoundTrip(data);
	}

	private void testRoundTrip(Dataset<Row> data) {
		data.show();
		data.printSchema();
		// Create the table
		StructField idfield = data.schema().fields()[0];
		assertEquals("id", idfield.name());
		StructField xfield = data.schema().fields()[1];
		assertEquals("x", xfield.name());

		JdbcDialect dialect = JdbcDialects.get(Config.databaseUrl());
		String idSql = JdbcUtils.getJdbcType(idfield.dataType(), dialect).databaseTypeDefinition();
		String xSql = forceType != null ? forceType : JdbcUtils
				.getJdbcType(xfield.dataType(), dialect)
				.databaseTypeDefinition();
		String qTable = dialect.quoteIdentifier(TABLE);
		String sql = "DROP TABLE IF EXISTS " + qTable + "; CREATE TABLE " + qTable + "(id " + idSql + ", x " + xSql + ")";
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		int maxPartitions = Config.sparkPartitions();
		if (maxPartitions > 0)
			data = data.coalesce(maxPartitions);

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

	@Test
	public void testDifferentOrder() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();

		stmt.execute("DROP TABLE IF EXISTS foo");
		// The types match but the columns are in a different order:
		stmt.execute("CREATE TABLE foo(z INT, y TEXT, x INT)");

		Dataset<Long> spine = spark.range(5);
		Dataset<Row> df = spine
				.withColumn("x", col("id").cast("INTEGER"))
				.withColumn("y", concat(lit("x"), col("id")))
				.drop(col("id"))
				.repartition(1);

		// Write to a table that has its columns in the wrong order
		df
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.save();
	}
}