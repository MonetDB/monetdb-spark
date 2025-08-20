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

import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.Step;

import java.io.ByteArrayOutputStream;

public abstract class Appender implements Step {
	protected final int index;
	protected Collector collector;
	protected ByteArrayOutputStream buffer;

	public Appender(int index) {
		this.index = index;
	}

	@Override
	public void init(Collector collector) {
		this.collector = collector;
		this.buffer = collector.getOrCreateBuffer(index);
	}
}
