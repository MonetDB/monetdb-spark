/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.common.Destination;

import java.io.Serial;
import java.io.Serializable;

/**
 * Passed from driver to executors, holds everything the executors must know.
 */
public class MonetDataWriterFactory implements DataWriterFactory, Serializable {
	@Serial
	private static final long serialVersionUID = 0L;

	private final Destination dest;
	private final StructType structType;
	private final ColumnType[] columnTypes;

	public MonetDataWriterFactory(Destination dest, StructType structType, ColumnType[] columnTypes) {
		this.dest = dest;
		this.structType = structType;
		this.columnTypes = columnTypes;
	}

	@Override
	public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
		return new MonetDataWriter(dest, structType, columnTypes);
	}
}
