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

import java.io.IOException;

public class DoubleExtractor extends Extractor {
	public DoubleExtractor(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) throws IOException {
		collector.scratchNull = row.isNullAt(index);
		if (collector.scratchNull)
			return;
		collector.scratchDouble = row.getDouble(index);
	}

}
