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

public class FloatAppender extends Appender {
	public FloatAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) {
		float f = collector.scratchNull ? Float.NaN : (float) collector.scratchDouble;
		int n = Float.floatToIntBits(f);
		byte[] scratch = collector.scratchBuffer;
		for (int i = 0; i < 4; i++) {
			scratch[i] = (byte) n;
			n >>= 8;
		}
		buffer.write(scratch, 0, 4);
	}
}
