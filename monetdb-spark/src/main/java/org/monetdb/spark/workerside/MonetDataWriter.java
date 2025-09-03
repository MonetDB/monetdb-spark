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
	private final StateTracker tracker;
	private final long startTime;

	public MonetDataWriter(Collector collector, Step[] steps, BinCopyUploader uploader, long batchSize) {
		this.collector = collector;
		this.steps = steps;
		this.uploader = uploader;
		this.tracker = new StateTracker();  // leave it in state null
		this.batchSize = batchSize;
		this.startTime = System.currentTimeMillis();
		collector.setOnStartUpload(() -> tracker.setState(State.Uploading));
		collector.setOnEndUpload(() -> tracker.setState(State.Server));
	}

	@Override
	public void write(InternalRow row) throws IOException {
		this.processRow(row);
	}

	public void processRow(SpecializedGetters row) throws IOException {
		if (tracker.getState() == null) {
			// We should just have set the tracker to State.Initializing in the constructor
			// but this is the critical path and an ==null check is cheaper than a
			// !=null&&==Initializing check.
			tracker.setState(State.Initializing, startTime);
			tracker.setState(State.Collecting);
		}
		tracker.addRow();
		runSteps(row);
		collector.endRow();
		if (collector.getRowCount() >= batchSize) {
			try {
				upload();
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

	private void upload() throws SQLException {
		tracker.addUpload();
		State prev = tracker.setState(State.Server);
		try {
			uploader.uploadBatch();
		} finally {
			tracker.setState(prev);
		}
//		System.err.println(tracker);
	}

	@Override
	public void abort() throws IOException {
		close();
	}

	@Override
	public WriterCommitMessage commit() throws IOException {
		State prev = tracker.setState(State.Server); // for upload()
		try {
			upload();
			tracker.setState(State.Committing);
			uploader.commit();
		} catch (SQLException e) {
			throw new IOException(e);
		} finally {
			tracker.setState(prev);
		}
		close();
//		System.err.println("END " + tracker);
		return null;
	}

	@Override
	public void close() throws IOException {
		tracker.setState(null);
		try {
			uploader.close();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

}
