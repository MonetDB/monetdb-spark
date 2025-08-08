/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy.conversions;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;
import java.math.BigInteger;

public class DecimalToHuge extends BinCopyConversion {
	private final int precision;
	private final int scale;

	public DecimalToHuge(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public void extract(SpecializedGetters row, int idx) throws IOException {
		Decimal dec = row.getDecimal(idx, precision, scale);
		BigInteger bi = dec.toJavaBigDecimal().unscaledValue();
		long lo = getLowBits(bi);
		long hi = getHighBits(bi);
		appendLE(lo);
		appendLE(hi);
	}

	private long getLowBits(BigInteger bi) {
		// .longValue() is documented to return the lower 64 bits.
		return bi.longValue();
	}

	private long getHighBits(BigInteger bi) {
		return bi.shiftRight(64).longValue();
	}

	@Override
	public byte[] constructNullRepresentation() {
		return constructIntegerNullRepresentation(16 );
	}

}
