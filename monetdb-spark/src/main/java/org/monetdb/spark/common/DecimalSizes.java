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

public class DecimalSizes {

	public static int bitsNeededForPrecision(int precision) {
		// 2^7 = 128 so 999 won't fit
		if (precision <= 2) {
			return 8;
		}

		// 2^15 = 32_768 so 99_999 won't fit
		if (precision <= 4) {
			return 16;
		}

		// 2^31 = 2_147_483_648 so 9_999_999_999 won't fit
		if (precision <= 9) {
			return 32;
		}

		// 2^63 = 9_223_372_036_854_775_808
		// so     9_999_999_999_999_999_999 won't fit
		if (precision <= 18) {
			return 64;
		}

		// 2^127 = 170_141_183_460_469_231_731_687_303_715_884_105_728
		// so      999_999_999_999_999_999_999_999_999_999_999_999_999
		if (precision <= 38) {
			return 128;
		}

		return -1;
	}
}
