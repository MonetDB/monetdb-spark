package org.monetdb.spark.source;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * Manage the process of writing to the table.
 *
 * Exists in the driver.
 */
public class MonetBatchWrite implements BatchWrite {
	public MonetBatchWrite(Destination dest, StructType structType, ColumnType[] columnTypes) {
	}

	@Override
	public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
		return null;
	}

	@Override
	public boolean useCommitCoordinator() {
		return BatchWrite.super.useCommitCoordinator();
	}

	@Override
	public void onDataWriterCommit(WriterCommitMessage message) {
		BatchWrite.super.onDataWriterCommit(message);
	}

	@Override
	public void commit(WriterCommitMessage[] messages) {

	}

	@Override
	public void abort(WriterCommitMessage[] messages) {

	}
}
