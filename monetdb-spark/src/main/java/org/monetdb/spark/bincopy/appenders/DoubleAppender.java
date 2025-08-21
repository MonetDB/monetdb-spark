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

public class DoubleAppender extends Appender {
	public DoubleAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) {
		double d = collector.scratchNull ? Double.NaN : collector.scratchDouble;
		long n = Double.doubleToLongBits(d);
		byte[] scratch = collector.scratchBuffer;
		for (int i = 0; i < 8; i++) {
			scratch[i] = (byte) n;
			n >>= 8;
		}
		buffer.write(scratch, 0, 8);
	}
}
