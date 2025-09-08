/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.Config;

import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MetricsCollectorTest {

	@Test
	public void testMetricsCollector() {
		SparkSession spark = Config.sparkSession();

		int nrows = 1_000;
		Dataset<Row> df = spark.range(nrows).withColumn("iseven", col("id").mod(lit(2)).equalTo(0));
		for (int i = 1; i <= 12; i++)
			df = df.withColumn("x" + i, col("id").cast("INTEGER"));

		// Create the table
		df
				.where(lit(false))
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("driver", "org.monetdb.jdbc.MonetDriver")
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.save();

		MetricsCollector collector = new MetricsCollector(spark);

		// Save the data
		df
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
//				.option("batchsize", 100)
				.save();

		Map<String, Long> metrics = collector.getMetrics();
		assertNotNull(metrics.get("monet.uploadCount"));
		assertNotNull(metrics.get("monet.rowCount"));
		assertNotNull(metrics.get("monet.millisInitializing"));
		assertNotNull(metrics.get("monet.millisCollecting"));
		assertNotNull(metrics.get("monet.millisUploading"));
		assertNotNull(metrics.get("monet.millisServer"));
		// assertNotNull(metrics.get("monet.millisCommitting")); sometimes not present because Spark omits the final few metrics
		assertNotNull(metrics.get("monet.millisTotal"));
	}
}