/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import java.util.Arrays;

/**
 * CustomMetric returned by the StateTracker.
 * <p>
 * By the time these metrics reach the driver, the name has disappeared
 * and the description has become the name.
 * <p>
 * This class implements CustomMetric so instances of it can be returned
 * from MonetWrite. For this to work, it needs a constructor with no arguments.
 */
public abstract class StateTrackerMetric implements CustomMetric {
	public final static StateTrackerMetric[] METRICS = new StateTrackerMetric[]{ //
			new MillisInitializing(), //
			new MillisCollecting(), //
			new MillisUploading(), //
			new MillisServer(), //
			new MillisTotal(), //
			new UploadCount(), //
			new RowCount(), //
	};

	@Override
	public abstract String name();

	@Override
	public String description() {
		return name();
	}

	@Override
	public String aggregateTaskMetrics(long[] taskMetrics) {
		long sum = Arrays.stream(taskMetrics).sum();
		return Long.toString(sum);
	}

	public abstract long extract(StateTracker tracker);

	public final TaskMetric extractMetric(StateTracker tracker) {
		return new TaskMetric(name(), extract(tracker));
	}

	public record TaskMetric(String name, long value) implements CustomTaskMetric {
	}

	public static class MillisInitializing extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisInitializing";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.duration(State.Initializing);
		}
	}

	public static class MillisCollecting extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisCollecting";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.duration(State.Collecting);
		}
	}

	public static class MillisUploading extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisUploading";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.duration(State.Uploading);
		}
	}

	public static class MillisServer extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisServer";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.duration(State.Server);
		}
	}

	public static class MillisCommitting extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisCommitting";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.duration(State.Committing);
		}
	}

	public static class MillisTotal extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.millisTotal";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.millisTotal();
		}
	}

	public static class UploadCount extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.uploadCount";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.uploadCount();
		}
	}

	public static class RowCount extends StateTrackerMetric {

		@Override
		public String name() {
			return "monet.rowCount";
		}

		@Override
		public long extract(StateTracker tracker) {
			return tracker.rowCount();
		}
	}


}
