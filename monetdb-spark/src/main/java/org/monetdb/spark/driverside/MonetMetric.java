/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.monetdb.spark.workerside.State;
import org.monetdb.spark.workerside.StateTracker;

import java.util.Arrays;

public abstract class MonetMetric implements CustomMetric {
	private final String name;
	private final String description;

	public MonetMetric(String name, String description) {
		this.name = name;
		this.description = description;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public String description() {
		return description;
	}

	@Override
	public String aggregateTaskMetrics(long[] measurements) {
		long sum = Arrays.stream(measurements).sum();
		return Long.toString(sum);
	}

	abstract public long extractValue(StateTracker tracker);

	public CustomTaskMetric extract(StateTracker tracker) {
		long value = extractValue(tracker);
		return new MonetTaskMetric(name(), value);
	}

	public static class MillisCollecting extends MonetMetric {
		public MillisCollecting() {
			super("millisCollecting", "milliseconds spent converting and accumulating data to upload");
		}

		@Override
		public long extractValue(StateTracker tracker) {
			return tracker.duration(State.Collecting);
		}
	}

}
