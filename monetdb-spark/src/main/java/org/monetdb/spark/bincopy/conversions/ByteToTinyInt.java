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

public class ByteToTinyInt extends BinCopyConversion {
	@Override
	public void extract(SpecializedGetters row, int idx) {
		byte b = row.getByte(idx);
		appendByte(b);
	}

	@Override
	public byte[] constructNullRepresentation() {
		return new byte[] { -0x80 };
	}
}
