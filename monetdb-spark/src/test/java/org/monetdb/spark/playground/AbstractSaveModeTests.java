package org.monetdb.spark.playground;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;
import org.monetdb.spark.util.MyAutoClose;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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

	protected void setUpTable(boolean mustExist) throws SQLException {
		stmt.execute("DROP TABLE IF EXISTS foo  -- prepare");
		if (mustExist) {
			// We create it as INT.
			// Spark will create it as BIGINT so we can check whether the table
			// is recreated by looking at the type.
			stmt.execute("CREATE TABLE foo(i INT)  -- prepare");
			stmt.execute("INSERT INTO foo VALUES (1)  -- prepare");
		}
	}

	protected void insertData(SaveMode mode) {
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
		setUpTable(true);
		insertData(SaveMode.Append);
		// INT because the table is never recreated
		verifyData("INT", true, true);
	}

	@Test
	public void testAppendWhenNotPresent() throws SQLException {
		setUpTable(false);
		insertData(SaveMode.Append);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testOverwriteWhenPresent() throws SQLException {
		setUpTable(true);
		insertData(SaveMode.Overwrite);
		// BIGINT because Spark replaces the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testOverwriteWhenNotPresent() throws SQLException {
		setUpTable(false);
		insertData(SaveMode.Overwrite);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}

}
