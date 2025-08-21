/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.monetdb.spark.bincopy.BinCopyUploader;

import java.io.IOException;
import java.sql.SQLException;

public class MonetDataWriter implements DataWriter<InternalRow> {
	private final Collector collector;
	private final Step[] steps;
	private final BinCopyUploader uploader;
	private final long batchSize;

	public MonetDataWriter(Collector collector, Step[] steps, BinCopyUploader uploader, long batchSize) {
		this.collector = collector;
		this.steps = steps;
		this.uploader = uploader;
		this.batchSize = batchSize;
	}

	@Override
	public void write(InternalRow row) throws IOException {
		this.processRow(row);
	}

	public void processRow(SpecializedGetters row) throws IOException {
		runSteps(row);
		collector.endRow();
		if (collector.getRowCount() >= batchSize) {
			try {
				uploader.uploadBatch();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	private void runSteps(SpecializedGetters row) throws IOException {
		for (Step step : steps) {
			step.exec(row);
		}
	}

	@Override
	public void abort() throws IOException {
		close();
	}

	@Override
	public WriterCommitMessage commit() throws IOException {
		try {
			uploader.uploadBatch();
			uploader.commit();
		} catch (SQLException e) {
			throw new IOException(e);
		}
		close();
		return null;
	}

	@Override
	public void close() throws IOException {
		try {
			uploader.close();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

}
