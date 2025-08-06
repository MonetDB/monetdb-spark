/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.bincopy.Collector;
import org.monetdb.spark.common.Destination;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.Statement;

public class MonetDataWriter implements DataWriter<InternalRow> {
	private final Destination dest;
	private final Collector collector;

	private MonetConnection conn;
	private Statement stmt;

	public MonetDataWriter(Destination dest, Converter[] converters) {
		this.dest = dest;
		try {
			collector = new Collector(converters);
		} catch (ConversionError e) {
			// Unlikely because {@link MonetWrite} already tried it.
			throw new RuntimeException(e);
		}

		try {
			conn = dest.connect();
			conn.setAutoCommit(false);
			conn.setUploadHandler(new Handler());
			stmt = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void write(InternalRow row) throws IOException {
		collector.convertRow(row);
	}


	@Override
	public void abort() throws IOException {
		close();
	}

	@Override
	public WriterCommitMessage commit() throws IOException {
		flushData();
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		close();
		return null;
	}

	private void flushData() {
		if (collector.getRowCount() == 0)
			return;
		String sql = collector.copyStatement(dest.getTable());
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		collector.clear();
	}


	@Override
	public void close() throws IOException {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException ignored) {
			}
			stmt = null;
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}

	private class Handler implements MonetConnection.UploadHandler {
		@Override
		public void handleUpload(MonetConnection.Upload handle, String filename, boolean textMode, long linesToSkip) throws IOException {
			int idx = Integer.parseInt(filename);
			OutputStream stream = handle.getStream();
			collector.writeCollected(idx, stream);
		}
	}
}
