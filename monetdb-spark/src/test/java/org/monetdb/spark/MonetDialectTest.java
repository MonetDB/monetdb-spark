/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;


/**
 * Test the mapping between Spark- and SQL types.
 * <p>
 * In particular, the mappings listed in
 * <a href="https://spark.apache.org/docs/4.0.0/sql-ref-datatypes.html#data-types">the Spark datatypes reference</a>.
 * <p>
 * Also learn which methods to call to extract a value of those types from a
 * {@link Row}.
 * <p>
 * This test uses the {@code df.read().format("jdbc")} data source,
 * not our own MonetDB specific one.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class MonetDialectTest {
	private final static String TABLE = "foo";
	private final byte YEAR = 0;
	private final byte MONTH = 1;
	private final byte DAY = 0;
	private final byte HOUR = 1;
	private final byte MINUTE = 2;
	private final byte SECOND = 3;

	@AutoClose
	Connection conn;
	@AutoClose
	Statement stmt;
	@AutoClose
	private SparkSession spark;

	/* the following are set by loadType() */
	private int savedPrecision;
	private int savedScale;
	private Row fetchedRow;
	private String savedTypeName;

	@BeforeAll
	public void setUpClass() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();
		spark = Config.sparkSession();
	}

	@BeforeEach
	public void setUp() {
		savedPrecision = -1;
		savedScale = -1;
		fetchedRow = null;
	}

	@Test
	public void testGetVersion() {
		String version = MonetDialect.getVersion();
		assertNotEquals("unknown", version);
		assertFalse(version.contains("unknown"));
		assertTrue(version.contains("."));
	}

	private JDBCType save(DataType sparkType) throws SQLException {
		StructField structField = DataTypes.createStructField("x", sparkType, false);
		StructType structType = DataTypes.createStructType(singletonList(structField));

		List<Row> rows = emptyList();
		Dataset<Row> df = spark.createDataFrame(rows, structType);
		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		try (var rs = stmt.executeQuery("SELECT * FROM " + TABLE)) {
			var md = rs.getMetaData();
			int typeInt = md.getColumnType(1);
			JDBCType sqlType = JDBCType.valueOf(typeInt);
			savedTypeName = md.getColumnTypeName(1);
			savedPrecision = md.getPrecision(1);
			savedScale = md.getScale(1);
			return sqlType;
		}
	}

	private DataType fetchDataFrame(Function<DataFrameReader, DataFrameReader> customizer) {
		DataFrameReader builder = spark
				.read()
				.format("jdbc")
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE);
		if (customizer != null)
			builder = customizer.apply(builder);
		Dataset<Row> df = builder.load();
		DataType sparkType = df.schema().fields()[0].dataType();
		assertEquals(1, df.count());
		List<Row> rows = df.collectAsList();
		fetchedRow = rows.get(0);
		return sparkType;
	}

	private DataType load(String colType, String value) throws SQLException {
		return load(colType, value, null);
	}

	private DataType load(String colType, String value, Function<DataFrameReader, DataFrameReader> customizer) throws SQLException {
		String sql;
		// Intentionally construct SQL with String concatenation.
		// Don't let JDBC mess with the types.
		sql = "DROP TABLE IF EXISTS " + TABLE + ";\n";
		sql += "CREATE TABLE " + TABLE + "(x " + colType + ")";
		stmt.execute(sql);

		sql = "INSERT INTO " + TABLE + " VALUES (" + value + ")";
		stmt.execute(sql);

		DataType sparkType = fetchDataFrame(customizer);
		assertFalse(fetchedRow.isNullAt(0));
		return sparkType;
	}

	private void assertNullWorks() throws SQLException {
		String sql = "UPDATE " + TABLE + " SET x = NULL";
		stmt.execute(sql);
		fetchDataFrame(null);
		assertTrue(fetchedRow.isNullAt(0));
	}

	@Test
	public void testLoadBOOLEAN() throws SQLException {
		DataType sparkType = load("BOOLEAN", "true");
		assertEquals(DataTypes.BooleanType, sparkType);
		assertTrue(fetchedRow.getBoolean(0));
		assertNullWorks();
	}

	@Test
	public void testSaveBooleanType() throws SQLException {
		JDBCType sqlType = save(DataTypes.BooleanType);
		assertEquals(JDBCType.BOOLEAN, sqlType);
	}

	@Test
	public void testLoadTINYINT() throws SQLException {
		DataType sparkType = load("TINYINT", "42");
		assertEquals(DataTypes.ByteType, sparkType);
		assertEquals((byte) 42, fetchedRow.getByte(0));
		assertNullWorks();
	}

	@Test
	public void testSaveByteType() throws SQLException {
		JDBCType sqlType = save(DataTypes.ByteType);
		assertEquals(JDBCType.SMALLINT, sqlType); // larger type!
	}


	@Test
	public void testLoadSMALLINT() throws SQLException {
		DataType sparkType = load("SMALLINT", "42");
		assertEquals(DataTypes.ShortType, sparkType);
		assertEquals((short) 42, fetchedRow.getShort(0));
		assertNullWorks();
	}

	@Test
	public void testSaveShortType() throws SQLException {
		JDBCType sqlType = save(DataTypes.ShortType);
		assertEquals(JDBCType.INTEGER, sqlType); // larger type!
	}

	@Test
	public void testLoadINTEGER() throws SQLException {
		DataType sparkType = load("INTEGER", "42");
		assertEquals(DataTypes.IntegerType, sparkType);
		assertEquals(42, fetchedRow.getInt(0));
		assertNullWorks();
	}

	@Test
	public void testSaveIntegerType() throws SQLException {
		JDBCType sqlType = save(DataTypes.IntegerType);
		assertEquals(JDBCType.BIGINT, sqlType); // larger type!
	}

	@Test
	public void testLoadBIGINT() throws SQLException {
		DataType sparkType = load("BIGINT", "42");
		assertEquals(DataTypes.LongType, sparkType);
		assertEquals(42, fetchedRow.getLong(0));
		assertNullWorks();
	}

	@Test
	public void testSaveLongType() throws SQLException {
		JDBCType sqlType = save(DataTypes.LongType);
		assertEquals(JDBCType.BIGINT, sqlType); // not a larger type!
	}

	@Test
	public void testLoadREAL() throws SQLException {
		DataType sparkType = load("REAL", "42.5");
		assertEquals(DataTypes.FloatType, sparkType);
		assertEquals((float) 42.5, fetchedRow.getFloat(0));
		assertNullWorks();
	}

	@Test
	public void testSaveFloatType() throws SQLException {
		JDBCType sqlType = save(DataTypes.FloatType);
		assertEquals(JDBCType.REAL, sqlType);
	}

	@Test
	public void testLoadDOUBLE() throws SQLException {
		DataType sparkType = load("DOUBLE", "42.5");
		assertEquals(DataTypes.DoubleType, sparkType);
		assertEquals(42.5, fetchedRow.getDouble(0));
		assertNullWorks();
	}

	@Test
	public void testSaveDoubleType() throws SQLException {
		JDBCType sqlType = save(DataTypes.DoubleType);
		assertEquals(JDBCType.DOUBLE, sqlType);
	}

	@Test
	public void testLoadDECIMAL_8_3() throws SQLException {
		DataType sparkType = load("DECIMAL(8, 3)", "12.34");
		DecimalType expectedType = DataTypes.createDecimalType(8, 3);
		assertEquals(expectedType, sparkType);
		BigDecimal dec = fetchedRow.getDecimal(0);
		BigDecimal expected = BigDecimal.valueOf(1234).scaleByPowerOfTen(-2).setScale(3, RoundingMode.DOWN);
		assertEquals("12.340", dec.toString());
		assertEquals(expected, dec);
	}

	@Test
	public void testSaveDecimalType_8_3() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDecimalType(8, 3));
		assertEquals(JDBCType.DECIMAL, sqlType);
		assertEquals(8, savedPrecision);
		assertEquals(3, savedScale);
	}

	@ParameterizedTest
	@ValueSource(strings = {"VARCHAR", "VARCHAR(10)", "TEXT", "CLOB", "STRING"})
	public void testLoadTextual(String decl) throws SQLException {
		DataType sparkType = load(decl, "'banana'");
		assertEquals(DataTypes.StringType, sparkType);
		assertEquals("banana", fetchedRow.getString(0));
		assertNullWorks();
	}

	@Test
	public void testLoadCHAR() throws SQLException {
		DataType sparkType = load("CHAR(8)", "'banana'");
		assertEquals(DataTypes.StringType, sparkType);
		// Padded with two spaces
		assertEquals("banana  ", fetchedRow.getString(0));
		assertNullWorks();
	}

	@Test
	public void testSaveStringType() throws SQLException {
		JDBCType sqlType = save(DataTypes.StringType);
		assertEquals(JDBCType.VARCHAR, sqlType);
	}

	@Test
	public void testLoadBLOB() throws SQLException {
		DataType sparkType = load("BLOB", "'000102FF'");
		assertEquals(DataTypes.BinaryType, sparkType);
		assertArrayEquals(new byte[]{0, 1, 2, -1}, (byte[]) fetchedRow.get(0));
		assertNullWorks();
	}

	@Test
	public void testSaveBinaryType() throws SQLException {
		JDBCType sqlType = save(DataTypes.BinaryType);
		assertEquals(JDBCType.VARBINARY, sqlType);
	}

	@Test
	public void testLoadDATE() throws SQLException {
		DataType sparkType = load("DATE", "'1989-11-09'");
		assertEquals(DataTypes.DateType, sparkType);
		// as java.sql.Date
		assertEquals(Date.valueOf("1989-11-09"), fetchedRow.getDate(0));
		// Spark docs say LocalTime is an option but getLocalDate doesn't seem to work with our jdbc?
		assertThrows(ClassCastException.class, () -> fetchedRow.getLocalDate(0));
		assertNullWorks();
	}

	@Test
	public void testSaveDateType() throws SQLException {
		JDBCType sqlType = save(DataTypes.DateType);
		assertEquals(JDBCType.DATE, sqlType);
	}

	@Test
	public void testLoadTIMESTAMP() throws SQLException {
		// SQL TIMESTAMP maps to Spark TimeStampNTZ.
		// By setting the customizer we change the time zone used by Spark to
		// something really weird. The connection used to create the table is
		// not affected.
		Function<DataFrameReader, DataFrameReader> customizer = (builder) -> builder.option("sessionInitStatement", "SET TIME ZONE INTERVAL '01:23' HOUR TO MINUTE");

		DataType sparkType = load("TIMESTAMP", "'1989-11-09 23:52:00'", customizer);
		assertEquals(DataTypes.TimestampNTZType, sparkType);
		// Within fetchedRow, the timestamp is stored as a java.time.LocalDateTime.
		// There doesn't seem to be a dedicated method to get it.
		var actual = (LocalDateTime) fetchedRow.get(0);
		// The expected localdatetime is not affected by the weird timezone.
		var expected = LocalDateTime.of(1989, 11, 9, 23, 52, 0);
		assertEquals(expected, actual);

		assertNullWorks();
	}

	@Test
	public void testSaveTimestampNTZType() throws SQLException {
		JDBCType sqlType = save(DataTypes.TimestampNTZType);
		assertEquals(JDBCType.TIMESTAMP, sqlType);
	}

	@Test
	public void testLoadTIMESTAMPTZ() throws SQLException {
		// SQL TIMESTAMPTZ maps to Spark TimeStamp, so it's session time zone aware.

		// timestamp including time zone. In Berlin it was 23:52:00+01:00.
		String original = "1989-11-09 22:52:00+00:00";
		Instant originalInstant = Instant.parse(original.replace(' ', 'T'));
		assertEquals(626655120_000L, originalInstant.toEpochMilli());

		// By setting the customizer we change the time zone used by Spark to
		// something really weird. The connection used to create the table is
		// not affected.
		Function<DataFrameReader, DataFrameReader> customizer = (builder) -> builder.option("sessionInitStatement", "SET TIME ZONE INTERVAL '01:23' HOUR TO MINUTE");

		DataType sparkType = load("TIMESTAMPTZ", "'" + original + "'", customizer);
		assertEquals(DataTypes.TimestampType, sparkType);

		// On the wire, the timestamp was transferred as 1989-11-10 00:15:00+01:23.
		java.sql.Timestamp actual = fetchedRow.getTimestamp(0);
		// But we get the right moment in time regardless
		assertEquals(626655120_000L, actual.getTime());
		assertEquals(originalInstant, actual.toInstant());
		assertNullWorks();
	}

	@Test
	public void testSaveTimestampType() throws SQLException {
		JDBCType sqlType = save(DataTypes.TimestampType);
		assertEquals(JDBCType.TIMESTAMP_WITH_TIMEZONE, sqlType);
	}


	// START JOERI
	@Test
	public void testLoadINTERVAL_YEAR() throws SQLException {
		DataType sparkType = load("INTERVAL YEAR", "INTERVAL '42' YEAR");
		assertEquals(DataTypes.createYearMonthIntervalType(YEAR, YEAR), sparkType);
		Period x = (Period) fetchedRow.get(0);
		assertEquals(Period.parse("P42Y"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalYear() throws SQLException {
		JDBCType sqlType = save(DataTypes.createYearMonthIntervalType(YEAR, YEAR));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval year", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_YEAR_MONTH() throws SQLException {
		DataType sparkType = load("INTERVAL YEAR TO MONTH", "INTERVAL '2-3' YEAR TO MONTH");
		assertEquals(DataTypes.createYearMonthIntervalType(YEAR, MONTH), sparkType);
		Period x = (Period) fetchedRow.get(0);
		assertEquals(Period.parse("P2Y3M"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalYearToMonth() throws SQLException {
		JDBCType sqlType = save(DataTypes.createYearMonthIntervalType(YEAR, MONTH));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval year to month", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_MONTH() throws SQLException {
		DataType sparkType = load("INTERVAL MONTH", "INTERVAL '7' MONTH");
		assertEquals(DataTypes.createYearMonthIntervalType(MONTH, MONTH), sparkType);
		Period x = (Period) fetchedRow.get(0);
		assertEquals(Period.parse("P7M"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalMonth() throws SQLException {
		JDBCType sqlType = save(DataTypes.createYearMonthIntervalType(MONTH, MONTH));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval month", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_DAY() throws SQLException {
		DataType sparkType = load("INTERVAL DAY", "INTERVAL '7' DAY");
		assertEquals(DataTypes.createDayTimeIntervalType(DAY, DAY), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("P7D"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalDay() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(DAY, DAY));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval day", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_DAY_HOUR() throws SQLException {
		DataType sparkType = load("INTERVAL DAY TO HOUR", "INTERVAL '2 3' DAY TO HOUR");
		assertEquals(DataTypes.createDayTimeIntervalType(DAY, HOUR), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("P2DT3H"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalDayToHour() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(DAY, HOUR));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval day to hour", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_DAY_MINUTE() throws SQLException {
		DataType sparkType = load("INTERVAL DAY TO MINUTE", "INTERVAL '2 3:4' DAY TO MINUTE");
		assertEquals(DataTypes.createDayTimeIntervalType(DAY, MINUTE), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("P2DT3H4M"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalDayToMinute() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(DAY, MINUTE));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval day to minute", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_DAY_SECOND() throws SQLException {
		DataType sparkType = load("INTERVAL DAY TO SECOND", "INTERVAL '2 3:4:5' DAY TO SECOND");
		assertEquals(DataTypes.createDayTimeIntervalType(DAY, SECOND), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("P2DT3H4M5S"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalDayToSecond() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(DAY, SECOND));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval day to second", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_HOUR() throws SQLException {
		DataType sparkType = load("INTERVAL HOUR", "INTERVAL '2' HOUR");
		assertEquals(DataTypes.createDayTimeIntervalType(HOUR, HOUR), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT2H"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalHour() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(HOUR, HOUR));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval hour", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_HOUR_MINUTE() throws SQLException {
		DataType sparkType = load("INTERVAL HOUR TO MINUTE", "INTERVAL '3:4' HOUR TO MINUTE");
		assertEquals(DataTypes.createDayTimeIntervalType(HOUR, MINUTE), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT3H4M"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalHourToMinute() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(HOUR, MINUTE));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval hour to minute", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_HOUR_SECOND() throws SQLException {
		DataType sparkType = load("INTERVAL HOUR TO SECOND", "INTERVAL '3:4:5' HOUR TO SECOND");
		assertEquals(DataTypes.createDayTimeIntervalType(HOUR, SECOND), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT3H4M5S"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalHourToSecond() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(HOUR, SECOND));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval hour to second", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_MINUTE() throws SQLException {
		DataType sparkType = load("INTERVAL MINUTE", "INTERVAL '4' MINUTE");
		assertEquals(DataTypes.createDayTimeIntervalType(MINUTE, MINUTE), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT4M"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalMinute() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(MINUTE, MINUTE));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval minute", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_MINUTE_SECOND() throws SQLException {
		DataType sparkType = load("INTERVAL MINUTE TO SECOND", "INTERVAL '4:5' MINUTE TO SECOND");
		assertEquals(DataTypes.createDayTimeIntervalType(MINUTE, SECOND), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT4M5S"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalMinuteToSecond() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(MINUTE, SECOND));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval minute to second", savedTypeName);
	}

	@Test
	public void testLoadINTERVAL_SECOND() throws SQLException {
		DataType sparkType = load("INTERVAL SECOND", "INTERVAL '6' SECOND");
		assertEquals(DataTypes.createDayTimeIntervalType(SECOND, SECOND), sparkType);
		Duration x = (Duration) fetchedRow.get(0);
		assertEquals(Duration.parse("PT6S"), x);
		assertNullWorks();
	}

	@Disabled("not supported by jdbc datasource")
	@Test
	public void testSaveIntervalSecond() throws SQLException {
		JDBCType sqlType = save(DataTypes.createDayTimeIntervalType(SECOND, SECOND));
		assertEquals(JDBCType.OTHER, sqlType);
		assertEquals("interval second", savedTypeName);
	}

// END JOERI


}
