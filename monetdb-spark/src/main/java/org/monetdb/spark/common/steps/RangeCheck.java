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
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.Step;

public abstract class RangeCheck implements Step {
	protected Collector collector;

	@Override
	public void init(Collector collector) {
		this.collector = collector;
	}

	@Override
	public void exec(SpecializedGetters row) {
		if (collector.scratchNull)
			return;
		if (isInRange())
			return;
		collector.scratchNull = true;
	}

	protected abstract boolean isInRange();
}
