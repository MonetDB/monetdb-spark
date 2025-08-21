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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;
import org.monetdb.spark.MockRow;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.MonetDataWriter;
import org.monetdb.spark.workerside.Step;

import java.io.IOException;
import java.sql.*;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.*;

public class UploadTests {
	final StructField boolField = new StructField("b", DataTypes.BooleanType, false, Metadata.empty());
	final StructField intField = new StructField("i", DataTypes.IntegerType, false, Metadata.empty());
	final StructField stringField = new StructField("s", DataTypes.StringType, false, Metadata.empty());

	@AutoClose
	Connection conn;
	@AutoClose
	Statement stmt;

	@Test
	public void testUploadingData() throws SQLException, ConversionError, IOException {
		// Create the table
		conn = Config.connectDatabase();
		conn.setAutoCommit(true);
		stmt = conn.createStatement();
		stmt.execute("DROP TABLE IF EXISTS foo");
		stmt.execute("CREATE TABLE foo(b BOOLEAN, i INTEGER, t TEXT)");

		// Check the column types are as exoected
		Destination dest = new Destination(Config.databaseUrl(), null, null, "foo");
		ColumnDescr[] colTypes = dest.getColumns();
		assertEquals(JDBCType.BOOLEAN, colTypes[0].getType());
		assertEquals(JDBCType.INTEGER, colTypes[1].getType());
		assertEquals(JDBCType.VARCHAR, colTypes[2].getType());

		// Create the DataWriter
		StructField[] sparkTypes = {boolField, intField, stringField};
		PlanBuilder builder = new PlanBuilder(colTypes);
		builder.plan(sparkTypes);
		Step[] steps = builder.getPlan();
		Collector collector = new Collector();
		collector.registerWithConverters(steps);
		BinCopyUploader uploader = new BinCopyUploader(dest, collector, builder.getColumns());
		long batchSize = Long.MAX_VALUE;
		MonetDataWriter dataWriter = new MonetDataWriter(collector, steps, uploader, batchSize);

		// Write data
		MockRow row1 = new MockRow(TRUE, 1, "one");
		MockRow row2 = new MockRow(FALSE, 2, "two");
		MockRow row3 = new MockRow(TRUE, 3, "three");
		// .processRow is the same as .write but with a more permissive type
		// so MockRow works.
		dataWriter.processRow(row1);
		dataWriter.processRow(row2);
		dataWriter.processRow(row3);
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
}
