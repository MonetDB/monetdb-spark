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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitioningTests {

	@BeforeAll
	public static void createTestData() throws SQLException {
		String sql = """
				DROP TABLE IF EXISTS foo;
				CREATE TABLE foo(id INT, n INT, d DATE, t TIMESTAMP WITH TIME ZONE);
				INSERT INTO foo
				SELECT
					id, n,
					DATE '2025-02-14' + INTERVAL '1' DAY * n AS d,
					TIMESTAMPTZ '2025-02-14 12:34:56.789+01:00' + INTERVAL '1' HOUR * n AS t
				FROM (SELECT value AS id, value % 1000 AS n FROM sys.generate_series(0, 15_000));
		""";
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		}
	}

	@Test
	public void testPartitioningInt() {
		testPartitioning("n", "0", "1000", 4);
	}

	@Test
	public void testPartitioningDate() {
		testPartitioning("d", "2025-02-14", "2027-11-11", 4);
	}

	@Test
	public void testPartitioningTimestamp() {
		testPartitioning("t", "2025-02-14 12:00:00", "2025-03-28 04:00:00", 4);
	}

	private static void testPartitioning(String partitionColumn, String lowerBound, String upperBound, int numPartitions) {
		SparkSession spark = Config.sparkSession();
		Dataset<Row> df = spark
				.read()
				.format("jdbc")
				.option("driver", "org.monetdb.jdbc.MonetDriver")
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.option("partitionColumn", partitionColumn)
				.option("lowerBound", lowerBound)
				.option("upperBound", upperBound)
				.option("numPartitions", numPartitions)
				.option("fetchsize", 1_000_000)
				.load()
				.cache();

		assertEquals(numPartitions, df.rdd().getNumPartitions());

		// did we get all the rows?
		long nrows = df.count();
		assertEquals(15_000, nrows);

		long ndistinct = df.select(col("id")).distinct().count();
		assertEquals(15_000, ndistinct);
	}
}
