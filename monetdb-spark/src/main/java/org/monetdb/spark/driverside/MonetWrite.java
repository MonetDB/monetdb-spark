/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;
import org.monetdb.spark.bincopy.PlanCompiler;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Step;

import java.sql.SQLException;

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
	private final Destination dest;
	private final int ncolumns;
	private final Step[] steps;
	private final long batchSize;

	public MonetWrite(Destination destination, StructType structType, long batchSize) {
		this.dest = destination;
		this.ncolumns = structType.fields().length;
		this.batchSize = batchSize;

		try {
			// We assume it exists, get the column types
			ColumnDescr[] columnDescrs = dest.getColumnTypes();
			steps = PlanCompiler.compile(structType.fields(), columnDescrs);
		} catch (SQLException | ConversionError e) {
			// Spark doesn't allow us to throw checked exceptions
			throw new RuntimeException(e);
		}
	}

	@Override
	public BatchWrite toBatch() {
		return new MonetBatchWrite(dest, ncolumns, steps, batchSize);
	}
}
