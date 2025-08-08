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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class DecimalSizesTest {

	public static boolean fits(int prec, int bits) {
		// it's sufficient to approximate it with floats
		double largestDecimal = Math.pow(10, prec) - 1.0;
		// bits-1 because we need to exclude the sign bit
		double largestRepr = Math.pow(2, bits - 1) - 1.0;
		return largestDecimal <= largestRepr;
	}

	static IntStream range38() {
		return IntStream.range(1, 38 + 1);
	}

	static int smallerSize(int bits) {
		return switch (bits) {
			case 8 -> throw new RuntimeException("8 is the smallest size");
			case 16 -> 8;
			case 32 -> 16;
			case 64 -> 32;
			case 128 -> 64;
			default -> throw new RuntimeException("invalid size: " + bits);
		};
	}

	@Test
	public void testTooBig() {
		assertEquals(-1, DecimalSizes.bitsNeededForPrecision(39));
	}

	@ParameterizedTest
	@MethodSource("range38")
	public void testPrecision(int prec) {
		int predictedSize = DecimalSizes.bitsNeededForPrecision(prec);
		assertTrue(fits(prec, predictedSize));
		if (predictedSize > 8) {
			int smaller = smallerSize(predictedSize);
			assertFalse(fits(prec, smaller));
		}
	}
}
