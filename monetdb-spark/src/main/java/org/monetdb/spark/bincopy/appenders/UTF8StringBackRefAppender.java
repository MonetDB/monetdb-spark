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
import org.monetdb.spark.util.BackRefEncoder;
import org.monetdb.spark.workerside.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;

public class UTF8StringBackRefAppender extends Appender {
    private final int backrefSize;
	private transient BackRefEncoder backRefEncoder;

	public UTF8StringBackRefAppender(int index, int backrefSize) {
		super(index);
        this.backrefSize = backrefSize;
	}

	@Override
	public void init(Collector collector) {
		super.init(collector);
        stream.onReset(this::resetEncoder);
		resetEncoder();
	}

	private void resetEncoder() {
		if (backRefEncoder == null)
			backRefEncoder = new BackRefEncoder(backrefSize);
		else
			backRefEncoder.reset();
	}

	@Override
	public void exec(SpecializedGetters ignored) throws IOException {
		if (collector.scratchNull) {
			backRefEncoder.write(null, stream);
		} else {
			ByteBuffer bb = collector.scratchUTF8String.getByteBuffer();
            backRefEncoder.write(bb, stream);
        }
	}
}
