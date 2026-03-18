/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy;

import org.jetbrains.annotations.NotNull;
import org.monetdb.spark.workerside.Collector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;

public class BinCopyFileDump implements Uploader {
	private final Path destDir;
	private final String copyFileName;
	private final String dataFilePrefix;
	private final String dataFileSuffix;
	private final Collector collector;
	private final BinCopySql sqlstmt;
	private final ArrayList<OutputStream> outs;
	private boolean closed;
	private Runnable onStartUpload;
	private Runnable onEndUpload;

	public BinCopyFileDump(String dumpdir, String dumpPrefix, Collector collector, BinCopySql sqlstmt, int partitionId, long taskId) throws IOException {
		this.collector = collector;
		this.sqlstmt = sqlstmt;
		String part = "part" + partitionId;

		Path topDir = Paths.get(dumpdir);
		destDir = topDir.resolve(part);
		copyFileName = "copy.sql";
		dataFilePrefix = "col";
		dataFileSuffix = ".bin";

		boolean haveDumpPrefix = dumpPrefix != null && !dumpPrefix.isEmpty();
		String prefix = (haveDumpPrefix ? dumpPrefix : ".") + File.separator + part + File.separator + dataFilePrefix;
		sqlstmt.nameMapper(i -> getDataFileName(i, prefix, dataFileSuffix));

		try {
			Files.createDirectory(topDir);
		} catch (FileAlreadyExistsException ignored) {
		}
		Files.createDirectory(destDir);
		String sql = sqlstmt.toString();
		Files.writeString(getCopyFilePath(), sql + ";", StandardCharsets.UTF_8);

		outs = new ArrayList<>(sqlstmt.getColumnCount());
		openOutFiles();
	}

	private @NotNull Path getCopyFilePath() {
		return destDir.resolve(copyFileName);
	}

	private @NotNull Path getDataFilePath(int i) {
		String name = getDataFileName(i, dataFilePrefix, dataFileSuffix);
		return destDir.resolve(name);
	}

	private @NotNull String getDataFileName(int i, String prefix, String suffix) {
		StringBuilder builder = new StringBuilder(prefix).append(i).append(".");
		for (char c : sqlstmt.getColumnName(i).toCharArray()) {
			if (!Character.isAlphabetic(c) && !Character.isDigit(c) && c != '_')
				c = '~';
			builder.append(c);
		}
		builder.append(suffix);
		return builder.toString();
	}

	private void openOutFiles() throws IOException {
		int n = sqlstmt.getColumnCount();
		int i = 0;
		try {
			while (i < n) {
				Path p = getDataFilePath(i);
				OutputStream s = Files.newOutputStream(p);
				s = collector.getCompressionSettings().wrap(s);
				outs.add(s);
				i++;
			}
		} catch (IOException e) {
			closeOutputFiles(true);
		}
	}

	private void closeOutputFiles(boolean ignoreErrors) throws IOException {
		if (closed)
			return;
		IOException toThrow = null;
		for (int i = 0; i < outs.size(); i++) {
			OutputStream s = outs.get(i);
			if (s != null) {
				try {
					s.close();
				} catch (IOException e) {
					if (toThrow == null)
						toThrow = e;
				}
				outs.set(i, null);
			}
		}
		closed = true;
		if (toThrow != null && !ignoreErrors)
			throw toThrow;
	}

	@Override
	public void uploadBatch() throws SQLException, IOException {
		if (onStartUpload != null)
			onStartUpload.run();
		try {
			collector.prepareUpload();
			for (int i = 0; i < outs.size(); i++) {
				collector.writeTo(i, outs.get(i));
			}
			collector.finishUpload();
		} finally {
			if (onEndUpload != null)
				onEndUpload.run();
		}
	}

	@Override
	public void commit() throws SQLException, IOException {
		for (OutputStream s : outs) {
			s.flush();
		}
		closeOutputFiles(false);
	}

	@Override
	public void close() throws IOException {
		if (closed) // was committed
			return;
		closeOutputFiles(true);
		silentDeleteFile(getCopyFilePath());
		for (int i = 0; i < sqlstmt.getColumnCount(); i++)
			silentDeleteFile(getDataFilePath(i));
		Files.delete(destDir);
	}

	private void silentDeleteFile(@NotNull Path p) {
		try {
			Files.delete(p);
		} catch (IOException ignored) {
		}
	}

	@Override
	public void setOnStartUpload(Runnable callback) {
		onStartUpload = callback;
	}

	@Override
	public void setOnEndUpload(Runnable callback) {
		onEndUpload = callback;

	}
}
