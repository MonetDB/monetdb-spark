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

import org.apache.commons.io.EndianUtils;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

import java.io.IOException;

public class FloatToFloat extends BinCopyConversion {
	@Override
	public void extract(SpecializedGetters row, int idx) throws IOException {
		float d = row.getFloat(idx);
		EndianUtils.writeSwappedFloat(buffer, d);
	}

	@Override
	public byte[] buildNullRepresentation() {
		int n = Float.floatToIntBits(Float.NaN);
		byte[] repr = new byte[8];
		// little endian
		for (int i = 0; i < 4; i++) {
			repr[i] = (byte) ((n >> 8 * i) % 256);
		}
		return repr;
	}
}
