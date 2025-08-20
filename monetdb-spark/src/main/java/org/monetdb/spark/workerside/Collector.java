/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.workerside;

import org.apache.spark.unsafe.types.UTF8String;
import org.monetdb.jdbc.MonetConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;

public final class Collector implements MonetConnection.UploadHandler {
	final ArrayList<ByteArrayOutputStream> buffers;
	private int rowCount = 0;

	public boolean scratchNull;
	public long scratchLong;
	public double scratchDouble;
	public BigInteger scratchBigInteger;
	public UTF8String scratchUTF8String;
	public byte[] scratchBuffer = new byte[16];

	public Collector() {
		buffers = new ArrayList<>();
	}

	public void registerWithConverters(Step[] steps) {
		for (Step step : steps) {
			step.init(this);
		}
	}

	public ByteArrayOutputStream getOrCreateBuffer(int idx) {
		while (buffers.size() <= idx)
			buffers.add(new ByteArrayOutputStream());
		return getBuffer(idx);
	}

	public ByteArrayOutputStream getBuffer(int idx) {
		return buffers.get(idx);
	}

	public void endRow() {
		rowCount++;
	}

	public int getRowCount() {
		return rowCount;
	}

	public long getTotalSize() {
		long totalSize = 0;
		for (ByteArrayOutputStream buffer : buffers)
			totalSize += buffer.size();
		return totalSize;
	}

	public void clear() {
		for (ByteArrayOutputStream buffer : buffers) {
			buffer.reset();
		}
		rowCount = 0;
	}

	@Override
	public void handleUpload(MonetConnection.Upload handle, String filename, boolean textMode, long linesToSkip) throws IOException {
		int idx = Integer.parseInt(filename);
		OutputStream stream = handle.getStream();
		buffers.get(idx).writeTo(stream);
	}
}
