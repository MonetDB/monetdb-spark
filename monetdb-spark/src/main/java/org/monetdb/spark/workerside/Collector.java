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

import org.monetdb.jdbc.MonetConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public final class Collector implements MonetConnection.UploadHandler {
	final ArrayList<ByteArrayOutputStream> buffers;
	private int rowCount = 0;

	public Collector() {
		buffers = new ArrayList<>();
	}

	public void registerWithConverters(Converter[] converters) {
		for (int i = 0; i < converters.length; i++) {
			converters[i].init(this, i);
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
