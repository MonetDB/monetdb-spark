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

import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;

public abstract class BinCopyConversion implements Converter {
	@Serial
	private static final long serialVersionUID = 0L;

	protected transient ByteArrayOutputStream buffer;
	protected transient byte[] nullRepresentation;

	@Override
	public void init(Collector collector, int idx) {
		buffer = collector.getOrCreateBuffer(idx);
		nullRepresentation = buildNullRepresentation();
	}

	@Override
	public void setNull(int idx) throws IOException {
		buffer.write(nullRepresentation);
	}

	public abstract byte[] buildNullRepresentation();
}
