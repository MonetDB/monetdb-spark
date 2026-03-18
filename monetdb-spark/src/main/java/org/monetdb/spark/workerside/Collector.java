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
import org.monetdb.spark.common.CompressionSettings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;

public final class Collector implements MonetConnection.UploadHandler {
	private final CompressionSettings compressionSettings;
	private final ArrayList<CollectorStream> collectorStreams;
	private final ArrayList<ByteArrayOutputStream> buffers;
	private int rowCount = 0;

	public boolean scratchNull;
	public long scratchLong;
	public double scratchDouble;
	public BigInteger scratchBigInteger;
	public UTF8String scratchUTF8String;
	public byte[] scratchByteArray;
	public final byte[] scratchBuffer = new byte[16];
	private Runnable onStartUpload;
	private Runnable onEndUpload;

	public Collector() {
		this(new CompressionSettings());
	}

	public Collector(CompressionSettings compressionSettings) {
		this.compressionSettings = compressionSettings;
		buffers = new ArrayList<>();
		collectorStreams = new ArrayList<>();
	}

	public void registerWithConverters(Step[] steps) {
		for (Step step : steps) {
			step.init(this);
		}
	}

	public int getStreamCount() {
		return buffers.size();
	}

	public CollectorStream getOrCreateStream(int idx) {
		// if idx == 2, collectorStreams 0, 1, 2 must exist so size must be 3
		for (int i = collectorStreams.size(); i <= idx; i++) {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			buffers.add(buffer);
			OutputStream inner = newInnerStream(i);
			collectorStreams.add(new CollectorStream(inner));
		}
		return collectorStreams.get(idx);
	}

	private OutputStream newInnerStream(int idx) {
		ByteArrayOutputStream buf = buffers.get(idx);
		buf.reset();
		return compressionSettings.wrap(buf);
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

	public void prepareUpload() throws IOException {
		for (CollectorStream s : collectorStreams) {
			s.flush();
			s.close();
		}
	}

	public void finishUpload() {
		int n = buffers.size();
		for (int i = 0; i < n; i++) {
			collectorStreams.get(i).setInner(newInnerStream(i));
		}
		rowCount = 0;
	}

	@Override
	public void handleUpload(MonetConnection.Upload handle, String filename, boolean textMode, long linesToSkip) throws IOException {
		int idx = Integer.parseInt(filename);
		onStartUpload.run();
		try {
			OutputStream stream = handle.getStream();
			writeTo(idx, stream);
			stream.close();
		} finally {
			onEndUpload.run();
		}
	}

	public void writeTo(int idx, OutputStream stream) throws IOException {
		buffers.get(idx).writeTo(stream);
	}

	public void setOnStartUpload(Runnable callback) {
		onStartUpload = callback;
	}

	public void setOnEndUpload(Runnable callback) {
		onEndUpload = callback;
	}

	public CompressionSettings getCompressionSettings() {
		return compressionSettings;
	}
}
