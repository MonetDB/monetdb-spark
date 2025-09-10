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

import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BinCopyUploader implements Uploader {
	private final Collector collector;
	private final MonetConnection conn;
	private final PreparedStatement stmt;

	public BinCopyUploader(Destination dest, Collector collector, BinCopySql sqlstmt) throws SQLException {
		String sep;
		this.collector = collector;
		this.conn = dest.connect();
		conn.setAutoCommit(false);
		conn.setUploadHandler(collector);

		String sql = sqlstmt.toString();
		stmt = conn.prepareStatement(sql);
	}

	@Override
	public void uploadBatch() throws SQLException {
		if (collector.getRowCount() == 0)
			return;
		stmt.execute();
		collector.clear();
	}

	@Override
	public void commit() throws SQLException {
		uploadBatch();
		conn.commit();
	}

	@Override
	public void close() throws SQLException {
		stmt.close();
		conn.close();
	}

	@Override
	public void setOnStartUpload(Runnable callback) {
		collector.setOnStartUpload(callback);
	}

	@Override
	public void setOnEndUpload(Runnable callback) {
		collector.setOnEndUpload(callback);
	}
}
