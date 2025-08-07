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
	private final Converter[] converters;
	private final BinCopyUploader uploader;
	private final long batchSize;

	public MonetDataWriter(Collector collector, Converter[] converters, BinCopyUploader uploader, long batchSize) {
		this.collector = collector;
		this.converters = converters;
		this.uploader = uploader;
		this.batchSize = batchSize;
	}

	@Override
	public void write(InternalRow row) throws IOException {
		this.processRow(row);
	}

	public void processRow(SpecializedGetters row) throws IOException {
		runConverters(row);
		collector.endRow();
		if (collector.getRowCount() >= batchSize) {
			try {
				uploader.uploadBatch();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	private void runConverters(SpecializedGetters row) throws IOException {
		for (int i = 0; i < converters.length; i++) {
			Converter converter = converters[i];
			if (row.isNullAt(i))
				converter.setNull(i);
			else
				converter.extract(row, i);
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
			throw new RuntimeException(e);
		}
	}

}
