/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.monetdb.spark.bincopy.BinCopySql;
import org.monetdb.spark.bincopy.PlanBuilder;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.StateTrackerMetric;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Everything we know about what to write where, and how.
 * <p>
 * Constructed in the driver when save() is finally called when writing a dataframe.
 * <p>
 * When we add support for truncate and overwrite (creating a new table), we will
 * do it here. For now, we just check if the dataframe type is suitable
 * to be converted to the column types of the existing table.
 * <p>
 * If we want to support more than one way of uploading, for example COPY INTO
 * in addition to COPY BINARY INTO, this is where we'd decide what to use.
 * <p>
 * The most important method is toBatch, which starts the actual
 * uploading of the data.
 */
public class MonetWrite implements Write {
	private final Parms parms;
	private final PlanBuilder builder;

	public MonetWrite(Parms parms) {
		this.parms = parms;

		try {
			// We assume it exists, get the column types
			Destination dest = parms.getDestination();
			ColumnDescr[] columnDescrs = dest.getColumns();
			builder = new PlanBuilder(columnDescrs, parms.isAllowOverflow());
			builder.plan(parms.getStructType());
		} catch (SQLException | ConversionError e) {
			// Spark doesn't allow us to throw checked exceptions
			throw new RuntimeException(e);
		}
	}

	@Override
	public BatchWrite toBatch() {
		BinCopySql sqlstmt = new BinCopySql(this.parms.getDestination().getTable(), builder.getColumns());
		return new MonetBatchWrite(parms, builder.getPlan(), sqlstmt);
	}

	@Override
	public CustomMetric[] supportedCustomMetrics() {
		CustomMetric[] superMetrics = Write.super.supportedCustomMetrics();
		CustomMetric[] customMetrics = StateTrackerMetric.METRICS;
		CustomMetric[] allMetrics = Stream
				.concat(Arrays.stream(superMetrics), Arrays.stream(customMetrics))
				.toArray(CustomMetric[]::new);
		return allMetrics;
	}
}
