package org.monetdb.spark.playground;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;
import org.monetdb.spark.util.MyAutoClose;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractSaveModeTests {
	private final StructType schema = new StructType(new StructField[]{new StructField("i", DataTypes.IntegerType, false, Metadata.empty())});
	@MyAutoClose
	protected Connection conn = null;
	@MyAutoClose
	protected Statement stmt = null;
	@MyAutoClose
	protected SparkSession spark = null;

	protected abstract String getSource();

	@BeforeEach
	public void setUp() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();
		spark = Config.sparkSession();
	}

	protected void setupFoo(boolean mustExist) throws SQLException {
		stmt.execute("DROP TABLE IF EXISTS foo -- prepare");
		if (mustExist) {
			// We create it as INT.
			// Spark will create it as BIGINT so we can check whether the table
			// is recreated by looking at the type.
			stmt.execute("CREATE TABLE foo(i INT)  -- prepare");
			stmt.execute("INSERT INTO foo VALUES (1)  -- prepare");
		}
	}

	protected void setupBar(boolean withData) throws SQLException {
		stmt.execute("ALTER TABLE foo ADD PRIMARY KEY (i)");
		stmt.execute("DROP TABLE IF EXISTS bar");
		stmt.execute("CREATE TABLE BAR(i INT, j INT REFERENCES foo(i))");
		if (withData)
			stmt.execute("INSERT INTO bar VALUES (10, 1)");
	}

	@AfterEach
	public void tearDown() throws SQLException {
		stmt.execute("DROP TABLE foo CASCADE -- tearDown");
	}

	protected void insertData(SaveMode mode) {
		insertData(mode, null);
	}

	protected void insertData(SaveMode mode, Map<String, String> opts) {
		// The table is created with the value 1 in it, we add 2.
		List<Row> data = List.of(RowFactory.create(2));
		Dataset<Row> df = spark.createDataFrame(data, schema);
		String source = getSource();
		DataFrameWriter<Row> writer = df
				.write()
				.format(source)
				.mode(mode)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo");
		if (opts != null)
				writer = writer.options(opts);
		writer.save();
	}

	protected void verifyData(String expectedType, boolean oldMustExist, boolean newMustExist) throws SQLException {
		boolean oldExists = false;
		boolean newExists = false;
		String dataType;

		try (ResultSet rs = stmt.executeQuery("SELECT * FROM foo")) {
			while (rs.next()) {
				int i = rs.getInt(1);
				oldExists |= i == 1;
				newExists |= i == 2;
			}
			dataType = rs.getMetaData().getColumnTypeName(1).toUpperCase();
		}

		assertEquals(oldMustExist, oldExists, "old data exists");
		assertEquals(newMustExist, newExists, "new data exists");
		assertEquals(expectedType, dataType);
	}

	@Test
	public void testAppendWhenPresent() throws SQLException {
		setupFoo(true);
		insertData(SaveMode.Append);
		// INT because the table is never recreated
		verifyData("INT", true, true);
	}

	@Test
	public void testAppendWhenNotPresent() throws SQLException {
		setupFoo(false);
		insertData(SaveMode.Append);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testOverwriteWhenPresent() throws SQLException {
		setupFoo(true);
		insertData(SaveMode.Overwrite);
		// BIGINT because Spark replaces the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testOverwriteWhenNotPresent() throws SQLException {
		setupFoo(false);
		insertData(SaveMode.Overwrite);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testOverwriteTruncateWhenPresent() throws SQLException {
		setupFoo(true);
		insertData(SaveMode.Overwrite, Map.of("truncate", "true"));
		// INT because Spark cleared but reused the table
		verifyData("INT", false, true);
	}

	@Test
	public void testOverwriteTruncateWhenNotPresent() throws SQLException {
		setupFoo(false);
		insertData(SaveMode.Overwrite, Map.of("truncate", "true"));
		// BIGINT because Spark creates the table (the value of 'truncate' is irrelevant)
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testForeignKeyWithoutTruncate() throws SQLException {
		setupFoo(true);
		setupBar(false);

		// Without truncate, Overwrite cannot DROP foo
		SQLException e = assertThrows(
				SQLException.class,
				() -> insertData(SaveMode.Overwrite)
		);
		// We'd expect 23000 but we get 40000. Whatever..
		assertEquals("40000", e.getSQLState());
	}

	@Test
	public void testForeignKeyWithTruncateNoData() throws SQLException {
		setupFoo(true);
		setupBar(false);

		// With current MonetDB, the TRUNCATE fails even though
		// there are no rows in 'bar' that actually reference 'foo'.
		// However, we get a different error code: 23000 instead of 40000.
		var opts = Map.of("truncate", "true");
		SQLException e = assertThrows(
				SQLException.class,
				() -> insertData(SaveMode.Overwrite, opts)
		);
		assertEquals("23000", e.getSQLState());
	}

	@Test
	public void testForeignKeyWithTruncateWithData() throws SQLException {
		setupFoo(true);
		setupBar(true);

		// The TRUNCATE fails because there's rows in bar that depend on foo.
		var opts = Map.of("truncate", "true");
		SQLException e = assertThrows(
				SQLException.class,
				() -> insertData(SaveMode.Overwrite, opts)
		);
		assertEquals("23000", e.getSQLState());
	}

	@Test
	public void testForeignKeyWithTruncateCascade() throws SQLException {
		setupFoo(true);
		setupBar(true);

		// Now the TRUNCATE succeeds because it takes 'bar' with it
		var opts = Map.of("truncate", "true", "cascadeTruncate", "true");
		insertData(SaveMode.Overwrite, opts);
		// Because of the TRUNCATE it should still be INT
		verifyData("INT", false, true);
	}
}
