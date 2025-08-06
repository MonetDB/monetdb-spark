/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy.conversions;

import org.monetdb.spark.bincopy.Collector;
import org.monetdb.spark.workerside.Converter;

import java.io.ByteArrayOutputStream;

public abstract class BinCopyConversion implements Converter {
	protected transient ByteArrayOutputStream buffer;

	@Override
	public void init(Collector collector, int idx) {
		buffer = collector.getBuffer(idx);
	}
}
