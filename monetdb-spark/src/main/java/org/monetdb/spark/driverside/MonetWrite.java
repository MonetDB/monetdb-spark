/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;
import org.monetdb.spark.bincopy.Conversions;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.Converter;

import java.sql.SQLException;

/**
 * Everything we know about what to write where, and how.
 *
 * Constructed in the driver when save() is finally called when writing a dataframe.
 * <p>
 * If we would support truncate and overwrite (creating a new table), we'd
 * do it here. For now, we just check if the dataframe type is suitable
 * to be converted to the column types of the existing table.
 * <p>
 * If we want to supoprt more than one way of uploading, for example COPY INTO
 * in addition to COPY BINARY INTO, this is where we'd decide what to use.
 * For now, we simply use org.monetdb.spark.bincopy.Collector}.
 * <p>
 * The most important method is toBatch, which starts the actual
 * uploading of the data.
 */
public class MonetWrite implements Write {
	private final Destination dest;
	private final Converter[] converters;

	public MonetWrite(Destination destination, StructType structType) {
		this.dest = destination;

		try {
			// We assume it exists, get the column types
			ColumnType[] columnTypes = dest.getColumnTypes();
			converters = Conversions.pickExtractors(structType.fields(), columnTypes);
		} catch (SQLException | ConversionError e) {
			// Spark doesn't allow us to throw checked exceptions
			throw new RuntimeException(e);
		}
	}

	@Override
	public BatchWrite toBatch() {
		return new MonetBatchWrite(dest, converters);
	}
}
