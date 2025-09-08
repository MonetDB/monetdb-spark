/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.util;

import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.collection.Iterator;

import java.util.HashMap;
import java.util.Map;

public class MetricsCollector extends SparkListener {
	private final HashMap<String, Long> metrics = new HashMap<>();

	public MetricsCollector(SparkSession spark) {
		this(spark.sparkContext());
	}

	public MetricsCollector(SparkContext context) {
		context.addSparkListener(this);
	}

	public Map<String, Long> getMetrics() {
		synchronized (metrics) {
			return new HashMap<>(metrics);
		}
	}

	@Override
	public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
		super.onTaskEnd(taskEnd);
		TaskMetrics taskMetrics = taskEnd.taskMetrics();
		if (taskMetrics == null)
			return;
		Iterator<AccumulatorV2<?, ?>> accumulators = taskMetrics.accumulators().iterator();
		synchronized (metrics) {
			while (accumulators.hasNext()) {
				AccumulatorV2<?, ?> acc = accumulators.next();
				String name;
				try {
					name = acc.name().get();
				} catch (IllegalStateException ignored) {
					continue;
				}
				Object obj = acc.value();
				if (name != null && obj instanceof Long value) {
					metrics.merge(name, value, Long::sum);
				}
			}
		}
	}
}
