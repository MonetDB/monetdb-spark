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

import org.monetdb.spark.common.Range;

public class LongRangeCheck extends RangeCheck {
	private final long lo;
	private final long hi;

	public LongRangeCheck(boolean allowOverflow, long lo, long hi) {
		super(allowOverflow);
		this.lo = lo;
		this.hi = hi;
	}

	public LongRangeCheck(boolean allowOverflow, Range range) {
		super(allowOverflow);
		this.lo = range.lo;
		this.hi = range.hi;
	}

	@Override
	public boolean isInRange() {
		return lo <= collector.scratchLong && collector.scratchLong <= hi;
	}
}
