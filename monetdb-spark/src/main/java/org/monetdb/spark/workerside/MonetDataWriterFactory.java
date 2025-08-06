/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
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
	private final Converter[] converters;

	public MonetDataWriterFactory(Destination dest, Converter[] converters) {
		this.dest = dest;
		this.converters = converters;
	}

	@Override
	public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
		return new MonetDataWriter(dest, converters);
	}
}
