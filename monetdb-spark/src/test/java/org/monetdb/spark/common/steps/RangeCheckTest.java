/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.common.steps;

import org.apache.spark.SparkException;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.*;

class RangeCheckTest {
	@Test
	public void testIntToDecimal8() throws SQLException, IOException {
		assertInRange(9999_9999, "INTEGER", "DECIMAL(8,0)");
		assertNotInRange(1_0000_0000, "INTEGER", "DECIMAL(8,0)");
	}

	@Test
	public void testHugeDecimalToSMALLINT() throws SQLException, IOException {
		BigDecimal fits = BigDecimal.valueOf(0x7F_FF);
		BigDecimal tooBig = BigDecimal.valueOf(0x80_00);
		assertInRange(fits, "DECIMAL(33,0)", "SMALLINT");
		assertNotInRange(tooBig, "DECIMAL(33,0)", "SMALLINT");
	}

	public void assertInRange(Object value, String sparkType, String sqlType) throws SQLException, IOException {
		boolean wasNull = tryToConvert(value, sparkType, sqlType, true);
		assertFalse(wasNull);
	}

	public void assertNotInRange(Object value, String sparkType, String sqlType) throws SQLException, IOException {
		boolean wasNull = tryToConvert(value, sparkType, sqlType, true);
		assertTrue(wasNull);
		SparkException exception = assertThrows(SparkException.class, () -> tryToConvert(value, sparkType, sqlType, false));
		Throwable cause = exception.getCause();
		assertInstanceOf(RuntimeException.class, cause);
		assertTrue(cause.toString().contains("out of range"));
	}

	private static boolean tryToConvert(Object value, String sparkType, String sqlType, boolean allowOverflow) throws SQLException, IOException {
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS foo");
			String sql = "CREATE TABLE foo(x " + sqlType + ")";
			stmt.execute(sql);

			try (SparkSession spark = Config.sparkSession()) {
				Dataset<Row> df = spark.range(1).withColumn("x", lit(value).cast(sparkType)).drop("id");
				df.repartition(1).write()
						.format("org.monetdb.spark")
						.mode(SaveMode.Append)
						.option("url", Config.databaseUrl())
						.option("dbtable", "foo")
						.option("allowoverflow", allowOverflow)
						.save();
			}

			try (ResultSet rs = stmt.executeQuery("SELECT x FROM foo")) {
				assertTrue(rs.next());
				Object ignored = rs.getObject(1);
				return rs.wasNull();
			}
		}

	}
}