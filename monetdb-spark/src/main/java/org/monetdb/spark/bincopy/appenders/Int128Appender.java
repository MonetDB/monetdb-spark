/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy.appenders;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

import java.math.BigInteger;

public class Int128Appender extends Appender {
	public Int128Appender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) {
		BigInteger bi = collector.scratchBigInteger;
		byte[] scratch = collector.scratchBuffer;

		if (collector.scratchNull) {
			for (int i = 0; i < 15; i++)
				scratch[i] = 0;
			scratch[15] = -0x80;
		} else {
			long lo = bi.longValue();
			for (int i = 0; i < 8; i++) {
				scratch[i] = (byte) lo;
				lo >>= 8;
			}
			long hi = bi.shiftRight(64).longValue();
			for (int i = 8; i < 16; i++) {
				scratch[i] = (byte) hi;
				hi >>= 8;
			}
		}
		buffer.write(scratch, 0, 16);
	}
}
