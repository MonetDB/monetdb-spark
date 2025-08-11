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
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

class MonetDialectTest {
	private final static String TABLE = "foo";

	@AutoClose
	private SparkSession spark;

	private JDBCType jdbcType;
	private int precision;
	private int scale;
	private Row row;

	@BeforeEach
	public void setUp() throws SQLException {
		spark = Config.sparkSession();
	}

	@Test
	public void testBoolean() throws SQLException {
		putData(DataTypes.BooleanType, Boolean.TRUE);
		assertEquals(JDBCType.BOOLEAN, jdbcType);
		assertEquals(DataTypes.BooleanType, getData());
		assertTrue(row.getBoolean(0));
	}

	@Test
	public void testShort() throws SQLException {
		putData(DataTypes.ShortType, (short)42);
		assertEquals(JDBCType.SMALLINT, jdbcType);
		assertEquals(DataTypes.ShortType, getData());
		assertEquals(42, row.getShort(0));
	}

	@Test
	public void testDecimal8_2() throws SQLException {
		DecimalType decimalType = DataTypes.createDecimalType(8, 2);
		Decimal decimalValue = Decimal.apply("12.34");
		putData(decimalType, decimalValue);
		assertEquals(JDBCType.DECIMAL, jdbcType);
		assertEquals(decimalType, getData());
		assertEquals(decimalValue.toJavaBigDecimal(), row.getDecimal(0));
	}

	private void putData(DataType dataType, Object value) throws SQLException {
		StructField structField = DataTypes.createStructField("x", dataType, false);
		StructType structType = DataTypes.createStructType(singletonList(structField));
		Row row = RowFactory.create(value);
		List<Row> rows = singletonList(row);
		Dataset<Row> df = spark.createDataFrame(rows, structType);
//		df.printSchema();
//		df.show();

		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.save();

		try (Connection conn = Config.connectDatabase();
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE)
		) {
			ResultSetMetaData md = rs.getMetaData();
			int typeInt = md.getColumnType(1);
			jdbcType = JDBCType.valueOf(typeInt);
			precision = md.getPrecision(1);
			scale = md.getScale(1);
		}
	}

	private DataType getData() {
		Dataset<Row> df = spark
				.read()
				.format("jdbc")
				.option("url", Config.databaseUrl())
				.option("dbtable", TABLE)
				.load();
		DataType foundType = df.schema().fields()[0].dataType();
		df.printSchema();
		df.show();

		List<Row> rows = df.collectAsList();
		row = rows.get(0);
		assertNotNull(row);
		return foundType;
	}
}