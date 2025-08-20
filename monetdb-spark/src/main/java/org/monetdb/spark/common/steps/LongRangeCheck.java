/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.common.steps;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.monetdb.spark.common.Range;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.Step;

import java.io.IOException;

public class LongRangeCheck implements Step {
	private final long lo;
	private final long hi;
	private Collector collector;

	public LongRangeCheck(long lo, long hi) {
		this.lo = lo;
		this.hi = hi;
	}

	public LongRangeCheck(Range range) {
		this.lo = range.lo;
		this.hi = range.hi;
	}

	@Override
	public void init(Collector collector) {
		this.collector = collector;
	}

	@Override
	public void exec(SpecializedGetters row) throws IOException {
		if (collector.scratchNull)
			return;
		if (collector.scratchLong >= lo && collector.scratchLong <= hi)
			return;
		// Overflow!
		// TODO add a setting to switch between throwing an error and setting it to NULL.
		collector.scratchNull = true;
	}
}
