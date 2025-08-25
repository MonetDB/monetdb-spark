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

import java.nio.ByteBuffer;

public class BinaryAppender extends Appender {
	private final byte[] NULL_REPRESENTATION = new byte[] { //
		-1, -1, -1, -1, //
		-1, -1, -1, -1, //
	};

	public BinaryAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters ignored) {
		if (collector.scratchNull) {
			buffer.write(NULL_REPRESENTATION, 0, 8);
		} else {
			byte[] value = collector.scratchByteArray;;
			byte[] scratch = collector.scratchBuffer;
			long len = value.length;
			scratch[0] = (byte) len;
			scratch[1] = (byte) (len >> 8);
			scratch[2] = (byte) (len >> 16);
			scratch[3] = (byte) (len >> 24);
			scratch[4] = 0; // array can't be longer than INT_MAX
			scratch[5] = 0;
			scratch[6] = 0;
			scratch[7] = 0;
			buffer.write(scratch, 0, 8);
			buffer.write(value, 0, value.length);
		}
	}
}
