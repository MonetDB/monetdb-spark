/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.Converter;
import org.monetdb.spark.workerside.MonetDataWriterFactory;

/**
 * Manage the process of writing to the table.
 * <p>
 * It runs on the driver.
 * Its main job is to create a DataWriterFactory. The Factory is serializable
 * so it can be shipped to the executors where it will be used to instantiate
 * an actual DataWriter.
 * <p>
 * Exists in the driver.
 */
public class MonetBatchWrite implements BatchWrite {
	private final Destination dest;
	private final Converter[] converters;

	public MonetBatchWrite(Destination dest, Converter[] converters) {
		this.dest = dest;
		this.converters = converters;
	}

	@Override
	public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
		return new MonetDataWriterFactory(dest, converters);
	}

	@Override
	public boolean useCommitCoordinator() {
		return true;
	}

	@Override
	public void onDataWriterCommit(WriterCommitMessage message) {
	}

	@Override
	public void commit(WriterCommitMessage[] messages) {
	}

	@Override
	public void abort(WriterCommitMessage[] messages) {
	}
}
