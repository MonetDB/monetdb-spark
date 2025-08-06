package org.monetdb.spark.bincopy;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.Config;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.workerside.ConversionError;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
	public void testLengthMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {boolCol, intCol};
		assertThrows(ConversionError.class, () -> new Collector(sparkTypes, colTypes));
	}

	@Test
	public void testTypeMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {boolCol, stringCol, stringCol};
		assertThrows(ConversionError.class, () -> new Collector(sparkTypes, colTypes));
	}

	private String formatCollected(int idx) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		col.writeCollected(idx, out);
		byte[] bytes = out.toByteArray();
		StringBuilder buf = new StringBuilder();
		for (byte b : bytes) {
			if (b == '$') {
				buf.append("$$");
			} else if (b >= ' ' && b < 127) {
				buf.append((char) b);
			} else {
				final String hex = "0123456789abcdef";
				char hi = hex.charAt(b / 16);
				char lo = hex.charAt(b % 16);
				buf.append('$');
				buf.append(hi);
				buf.append(lo);
			}
		}
		return buf.toString();
	}

	@Test
	public void testUTF8String() throws IOException {
		String str = "AAP";
		byte[] aapBytes = {0x41, 0x41, 0x50};
		assertArrayEquals(aapBytes, str.getBytes(StandardCharsets.UTF_8));
		UTF8String utf8str = UTF8String.fromString(str);
		byte[] ubytes = utf8str.getBytes();
		assertArrayEquals(aapBytes, ubytes);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		utf8str.writeTo(out);
		byte[] sbytes = out.toByteArray();
		assertArrayEquals(aapBytes, sbytes);

	}

	@Test
	public void testCollector() throws ConversionError, IOException {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {boolCol, intCol, stringCol};
		col = new Collector(sparkTypes, colTypes);
		assertEquals("COPY LITTLE ENDIAN BINARY INTO foo FROM '0', '1', '2' ON CLIENT", col.copyStatement("foo"));
		assertEquals("", formatCollected(0));
		assertEquals("", formatCollected(1));
		assertEquals("", formatCollected(2));

		TestRow row = new TestRow(TRUE, 1, "one");
		col.convertRow(row);

		row = new TestRow(FALSE, 2, "two");
		col.convertRow(row);

		row = new TestRow(TRUE, 3, "three");
		col.convertRow(row);


		// booleans are a single byte
		assertEquals("$01$00$01", formatCollected(0));
		// integers are 4 bytes, little endian
		assertEquals("$01$00$00$00$02$00$00$00$03$00$00$00", formatCollected(1));
		// strings are NUL separated
		assertEquals("one$00two$00three$00", formatCollected(2));

		assertEquals(3, col.getRowCount());
		int col1Size = 3; // one byte per row
		int col2Size = 12; // four bytes per row
		int col3Size = 3 + 1 + 3 + 1 + 5 + 1;
		assertEquals(col1Size + col2Size + col3Size, col.getTotalSize());
	}

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
			col = new Collector(sparkTypes, colTypes);
			col.convertRow(new TestRow(TRUE, 1, "one"));
			col.convertRow(new TestRow(FALSE, 2, "two"));
			col.convertRow(new TestRow(TRUE, 3, "three"));

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
}