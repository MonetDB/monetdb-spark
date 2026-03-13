/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;
import org.monetdb.spark.MockRow;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.CompressionSettings;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.util.MyAutoClose;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.MonetDataWriter;
import org.monetdb.spark.workerside.Step;

import java.io.IOException;
import java.sql.*;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class UploadTests {
	final StructField boolField = new StructField("b", DataTypes.BooleanType, false, Metadata.empty());
	final StructField intField = new StructField("i", DataTypes.IntegerType, false, Metadata.empty());
	final StructField stringField = new StructField("t", DataTypes.StringType, false, Metadata.empty());

	@MyAutoClose
	Connection conn;
	@MyAutoClose
	Statement stmt;

	@BeforeEach
	public void setup() throws SQLException {
		conn = Config.connectDatabase();
		conn.setAutoCommit(true);
		stmt = conn.createStatement();
	}

	@Test
	public void testUploadingData() throws SQLException, ConversionError, IOException {
		testUploadingData(new CompressionSettings());
	}

	@Test
	public void testUploadingCompressedData() throws SQLException, ConversionError, IOException {
		assumeTrue(Config.supportsCompression(conn, "lz4"), "LZ4 not supported");
		testUploadingData(new CompressionSettings("lz4"));
	}

	public void testUploadingData(CompressionSettings compression) throws SQLException, ConversionError, IOException {
		// Create the table
		stmt.execute("DROP TABLE IF EXISTS foo");
		stmt.execute("CREATE TABLE foo(b BOOLEAN, i INTEGER, t TEXT)");

		// Check the column types are as expected
		Destination dest = new Destination(Config.databaseUrl(), null, null, "foo");
		ColumnDescr[] colTypes = dest.getColumns();
		assertEquals(JDBCType.BOOLEAN, colTypes[0].type());
		assertEquals(JDBCType.INTEGER, colTypes[1].type());
		assertEquals(JDBCType.VARCHAR, colTypes[2].type());

		// Create the DataWriter
		StructField[] sparkTypes = {boolField, intField, stringField};
		PlanBuilder builder = new PlanBuilder(colTypes, false);
		builder.plan(sparkTypes);
		Step[] steps = builder.getPlan();
		Collector collector = new Collector(compression);
		collector.registerWithConverters(steps);
		String identifier = "uploadtest";
		BinCopySql sqlstmt = new BinCopySql(dest.getTable(), builder.getColumns())
				.identifier(identifier)
				.compression(compression);
		BinCopyUploader uploader = new BinCopyUploader(dest, collector, sqlstmt);
		long batchSize = Long.MAX_VALUE;
		MonetDataWriter dataWriter = new MonetDataWriter(collector, steps, uploader, false, identifier, batchSize);

		// Write data
		MockRow row1 = new MockRow(TRUE, 1, "one");
		MockRow row2 = new MockRow(FALSE, 2, "two");
		MockRow row3 = new MockRow(TRUE, 3, "three");
		// .processRow is the same as .write but with a more permissive type
		// so MockRow works.
		dataWriter.doWrite(row1);
		dataWriter.doWrite(row2);
		dataWriter.doWrite(row3);
		dataWriter.commit();
		dataWriter.close();

		// Verify data
		try (ResultSet rs = stmt.executeQuery("SELECT * FROM foo ORDER BY i")) {
			assertTrue(rs.next());
			assertEquals(TRUE, rs.getBoolean(1));
			assertEquals(1, rs.getInt(2));
			assertEquals("one", rs.getString(3));

			assertTrue(rs.next());
			assertEquals(FALSE, rs.getBoolean(1));
			assertEquals(2, rs.getInt(2));
			assertEquals("two", rs.getString(3));

			assertTrue(rs.next());
			assertEquals(TRUE, rs.getBoolean(1));
			assertEquals(3, rs.getInt(2));
			assertEquals("three", rs.getString(3));

			assertFalse(rs.next());
		}
	}

	@Test
	public void testBackslashColumn() throws SQLException {
		stmt.execute("DROP TABLE IF EXISTS foo");
		// the column name has 4 characters: i backslash space i
		stmt.execute("CREATE TABLE foo (\"i\\ i\" INT)");

		Destination dest = new Destination(Config.databaseUrl(), null, null, "foo");
		ColumnDescr[] colTypes = dest.getColumns();
		assertEquals("i\\ i", colTypes[0].name());
	}

	@Test
	public void testCompressionSupportCheck() throws SQLException, IOException {
		String algo = "lz4";
		boolean serverSupportsLZ4 = Config.supportsCompression(conn, algo);
		boolean detectedLZ4Support;

		stmt.execute("DROP TABLE IF EXISTS foo");
		stmt.execute("CREATE TABLE foo(i INT)");

		try (SparkSession spark = Config.sparkSession()) {
			Dataset<Row> df = spark.range(1_000)
					.withColumn("i", col("id").mod(100).cast("Integer"))
					.drop("id");

			try {
				df.write()
					.format("org.monetdb.spark")
					.mode(SaveMode.Append)
					.option("url", Config.databaseUrl())
					.option("compression", "lz4")
					.option("dbtable", "foo")
					.save();
				detectedLZ4Support = true;
			} catch (RuntimeException e) {
				if (!e.getMessage().contains("Server does not support compression algorithm"))
					throw e;
				detectedLZ4Support = false;
			}

			assertEquals(serverSupportsLZ4, detectedLZ4Support);
		}

	}
}
