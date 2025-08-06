package org.monetdb.spark.bincopy;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.Config;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.workerside.ConversionError;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.*;

class CollectorTest {
	final StructField boolField = new StructField("b", DataTypes.BooleanType, false, Metadata.empty());
	final StructField intField = new StructField("i", DataTypes.IntegerType, false, Metadata.empty());
	final StructField stringField = new StructField("s", DataTypes.StringType, false, Metadata.empty());
	final ColumnType boolCol = new ColumnType(JDBCType.BOOLEAN);
	final ColumnType intCol = new ColumnType(JDBCType.INTEGER);
	final ColumnType stringCol = new ColumnType(JDBCType.VARCHAR);

	Collector col;

	@Test
	public void testUseCollector() throws SQLException, ConversionError {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {null, null, null};
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			// Create the table
			stmt.execute("DROP TABLE IF EXISTS foo");
			stmt.execute("CREATE TABLE foo(b BOOLEAN, i INTEGER, t TEXT)");
			// Retrieve its type
			try (ResultSet rs = stmt.executeQuery("SELECT * FROM foo WHERE false")) {
				ResultSetMetaData md = rs.getMetaData();
				assertEquals(md.getColumnCount(), colTypes.length);
				for (int i = 0; i < colTypes.length; i++) {
					int typeInt = md.getColumnType(i + 1);
					JDBCType typeEnum = JDBCType.valueOf(typeInt);
					ColumnType colType = new ColumnType(typeEnum);
					colTypes[i] = colType;
				}
			}
			// Collect some data
			col = new Collector(Conversions.pickExtractors(sparkTypes, colTypes));
			MockRow row1 = new MockRow(TRUE, 1, "one");
			MockRow row2 = new MockRow(FALSE, 2, "two");
			MockRow row3 = new MockRow(TRUE, 3, "three");
			convertRow(row1);
			convertRow(row2);
			convertRow(row3);

			// To upload it, we first register an upload handler
			MonetConnection.UploadHandler uploadHandler = (handle, name, textMode, linesToSkip) -> {
				int idx = Integer.parseInt(name);
				OutputStream stream = handle.getStream();
				col.writeCollected(idx, stream);
			};
			MonetConnection monet = conn.unwrap(MonetConnection.class);
			monet.setUploadHandler(uploadHandler);
			// Then we execute the query
			String sql = col.copyStatement("foo");
			stmt.execute(sql);

			// Verify the data arrived
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

	private void convertRow(MockRow row1) {
		col.convertRow(row1);
	}
}