/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.monetdb.spark.bincopy.BinCopyUploader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;

public class MonetDataWriter implements DataWriter<InternalRow> {
	private final Collector collector;
	private final Step[] steps;
	private final BinCopyUploader uploader;
	private final long batchSize;
	private final boolean immediateCommit;
	private final StateTracker tracker;
	private final long startTime;

	public MonetDataWriter(Collector collector, Step[] steps, BinCopyUploader uploader, boolean immediateCommit, String identifier, long batchSize) {
		this.collector = collector;
		this.steps = steps;
		this.uploader = uploader;
		this.immediateCommit = immediateCommit;
		this.tracker = new StateTracker(identifier);  // leave it in state null
		this.batchSize = batchSize;
		this.startTime = System.currentTimeMillis();
		collector.setOnStartUpload(() -> tracker.setState(State.Uploading));
		collector.setOnEndUpload(() -> tracker.setState(State.Server));
	}

	public void doWrite(SpecializedGetters row) throws SQLException {
		if (tracker.getState() == null) {
			// We should just have set the tracker to State.Initializing in the constructor
			// but this is the critical path and an ==null check is cheaper than a
			// !=null&&==Initializing check.
			tracker.setState(State.Initializing, startTime);
			tracker.setState(State.Collecting);
		}
		runSteps(row);
		collector.endRow();
		tracker.addRow();
		if (collector.getRowCount() >= batchSize) {
			doUpload();
			if (immediateCommit)
				doCommit();
		}
	}

	private void runSteps(SpecializedGetters row) {
		for (Step step : steps) {
			step.exec(row);
		}
	}

	private void doUpload() throws SQLException {
		tracker.addUpload();
		State prev = tracker.setState(State.Server);
		try {
			uploader.uploadBatch();
		} finally {
			tracker.setState(prev);
		}
	}

	private void doCommit() throws SQLException {
		State prev = tracker.setState(State.Committing);
		try {
			uploader.commit();
		} finally {
			tracker.setState(prev);
		}
	}

	@Override
	public void write(InternalRow row) throws IOException {
		try {
			this.doWrite(row);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void abort() throws IOException {
		close();
	}

	@Override
	public WriterCommitMessage commit() throws IOException {
		try {
			doUpload();
			doCommit();
		} catch (SQLException e) {
			throw new IOException(e);
		}
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

	@Override
	public CustomTaskMetric[] currentMetricsValues() {
		CustomTaskMetric[] superMetrics = DataWriter.super.currentMetricsValues();
		Stream<CustomTaskMetric> customMetrics = Arrays
				.stream(StateTrackerMetric.METRICS)
				.map(m -> m.extractMetric(tracker));
		Stream<CustomTaskMetric> allMetrics = Stream.concat(Arrays.stream(superMetrics), customMetrics);
		return allMetrics.toArray(CustomTaskMetric[]::new);
	}
}
