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

import java.io.IOException;
import java.nio.ByteBuffer;

public class UTF8StringAppender extends Appender {
	public UTF8StringAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters ignored) throws IOException {
		if (collector.scratchNull) {
			stream.write(-0x80);
		} else {
			ByteBuffer bb = collector.scratchUTF8String.getByteBuffer();
			stream.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
		}
		stream.write(0);
	}
}
