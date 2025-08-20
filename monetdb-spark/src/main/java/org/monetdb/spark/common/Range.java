/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.common;

public class Range {
	public final long lo;
	public final long hi;

	public Range(long lo, long hi) {
		this.lo = lo;
		this.hi = hi;
	}

	public Range(long hi) {
		this.hi = hi;
		this.lo = -hi;
	}

	public static Range forDecimalWidth(int width) {
		long hi = 0;
		for (int i = 0; i < width; i++) {
			long n = 10 * hi + 9;
			if (n < hi)
				throw new RuntimeException("overflow, width " + width + " too high");
			hi = n;
		}
		return new Range(hi);
	}

	public static Range forIntegerSymmetric(int width) {
		if (width > 64)
			throw new RuntimeException("overflow, bit width " + width + " too high");
		// slow but avoids overflows
		long hi = 0;
		for (int i = 0; i < width - 1; i++)
			hi = 2 * hi + 1;
		return new Range(hi);
	}

	public static Range forIntegerFull(int width) {
		Range r = Range.forIntegerSymmetric(width);
		return new Range(r.lo - 1, r.hi);
	}

	public boolean contains(Range other) {
		return lo <= other.lo && other.hi <= hi;
	}
}
