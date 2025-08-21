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
import org.monetdb.spark.workerside.ConversionError;

public abstract class IntLikeExtractor extends Extractor {

	public IntLikeExtractor(int index) {
		super(index);
	}

	public static IntLikeExtractor forSize(int index, int size) throws ConversionError {
		return switch (size) {
			case 8 -> new ByteExtractor(index);
			case 16 -> new ShortExtractor(index);
			case 32 -> new IntegerExtractor(index);
			case 64 -> new LongExtractor(index);
			default -> throw new ConversionError("invalid integer size " + size);
		};
	}

	@Override
	public final void exec(SpecializedGetters row) {
		collector.scratchNull = row.isNullAt(index);
		if (!collector.scratchNull)
			collector.scratchLong = doExtract(row);
	}

	protected abstract long doExtract(SpecializedGetters row);
}
