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

public class UTF8StringAppender extends Appender {
	public UTF8StringAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters ignored) {
		if (collector.scratchNull) {
			buffer.write(-0x80);
		} else {
			ByteBuffer bb = collector.scratchUTF8String.getByteBuffer();
			buffer.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
		}
		buffer.write(0);
	}
}
