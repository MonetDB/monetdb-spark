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

import org.apache.commons.io.EndianUtils;
import org.jetbrains.annotations.NotNull;
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

	protected static byte @NotNull [] constructIntegerNullRepresentation(int n) {
		byte[] repr = new byte[n];
		repr[n - 1] = -0x80;
		return repr;
	}

	@Override
	public void init(Collector collector, int idx) {
		buffer = collector.getOrCreateBuffer(idx);
		nullRepresentation = constructNullRepresentation();
	}

	@Override
	public void setNull(int idx) throws IOException {
		buffer.write(nullRepresentation);
	}

	public abstract byte[] constructNullRepresentation();

	void appendByte(byte b) {
		buffer.write(b);
	}

	protected void appendByte(int numeric) {
		appendByte((byte) numeric);
	}

	protected void appendBytes(byte[] bytes) throws IOException {
		buffer.write(bytes);
	}

	protected void appendLE(short n) throws IOException {
		EndianUtils.writeSwappedShort(buffer, n);
	}

	protected void appendLE(int i) throws IOException {
		EndianUtils.writeSwappedInteger(buffer, i);
	}

	protected void appendLE(long n) throws IOException {
		EndianUtils.writeSwappedLong(buffer, n);
	}

	protected void appendLE(float d) throws IOException {
		EndianUtils.writeSwappedFloat(buffer, d);
	}

	protected void appendLE(double d) throws IOException {
		EndianUtils.writeSwappedDouble(buffer, d);
	}
}
