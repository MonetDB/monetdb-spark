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

import java.math.BigInteger;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;

public class BigIntRangeCheck extends RangeCheck {
	private final BigInteger hi;
	private final BigInteger lo;

	public BigIntRangeCheck(int precision) {
		hi = TEN.pow(precision).subtract(ONE);
		lo = hi.negate();
	}

	public BigIntRangeCheck(Range range) {
		hi = BigInteger.valueOf(range.hi);
		lo = BigInteger.valueOf(range.lo);
	}

	@Override
	protected boolean isInRange() {
		BigInteger bi = collector.scratchBigInteger;
		int cmp1 = lo.compareTo(bi);
		int cmp2 = bi.compareTo(hi);
		return cmp1 <= 0 && cmp2 <= 0;
	}
}
