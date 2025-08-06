/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

public class BooleanToBoolean extends BinCopyExtractor {
	@Override
	public void extract(SpecializedGetters row, int idx) {
		boolean b = row.getBoolean(idx);
		int numeric = b ? 1 : 0;
		buffer.write(numeric);
	}
}
