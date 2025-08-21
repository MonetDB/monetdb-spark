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

public abstract class IntegerLikeAppender extends Appender {

	public IntegerLikeAppender(int index) {
		super(index);
	}

	public static IntegerLikeAppender forIntSize(int index, int size) {
		IntegerLikeAppender appender = switch (size) {
			case 8 -> new Int8Appender(index);
			case 16 -> new Int16Appender(index);
			case 32 -> new Int32Appender(index);
			case 64 -> new Int64Appender(index);
			default -> null;
		};
		return appender;
	}

	protected abstract int sizeInBytes();

	@Override
	public void exec(SpecializedGetters ignored) {
		int n = sizeInBytes();
		byte[] scratch = collector.scratchBuffer;
		long value = collector.scratchLong;
		if (collector.scratchNull) {
			for (int i = 0; i < n - 1; i++)
				scratch[i] = 0x00;
			scratch[n - 1] = -0x80;
		} else {
			// little endian
			for (int i = 0; i < n; i++) {
				scratch[i] = (byte) value;
				value >>= 8;
			}
		}
		buffer.write(scratch, 0, n);
	}
}
