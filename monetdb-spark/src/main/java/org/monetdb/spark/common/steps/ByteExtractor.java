/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.common.steps;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

public class ByteExtractor extends IntLikeExtractor {
	public ByteExtractor(int index) {
		super(index);
	}

	@Override
	protected long doExtract(SpecializedGetters row) {
		return row.getByte(index);
	}
}
