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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BinCopyUploader {
	private final Collector collector;
	private final MonetConnection conn;
	private final PreparedStatement stmt;

	public BinCopyUploader(Destination dest, Collector collector, String identifier, String[] columns) throws SQLException {
		String sep;
		this.collector = collector;
		this.conn = dest.connect();
		conn.setAutoCommit(false);
		conn.setUploadHandler(collector);

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		pw.printf("COPY /* %s */ LITTLE ENDIAN BINARY INTO %s (", identifier, dest.getTable());
		sep = "";
		for (String col: columns) {
			pw.printf("%s\"%s\"", sep, col.replace("\"", "\"\""));
			sep = ", ";
		}
		pw.println(")");

		pw.print("FROM ");
		sep = "";
		for (int i = 0; i < columns.length; i++) {
			pw.printf("%s'%d'", sep, i);
			sep = ", ";
		}
		pw.println();
		pw.println("ON CLIENT");

		String sql = sw.toString();
		stmt = conn.prepareStatement(sql);
	}

	public void uploadBatch() throws SQLException {
		if (collector.getRowCount() == 0)
			return;
		stmt.execute();
		collector.clear();
	}

	public void commit() throws SQLException {
		uploadBatch();
		conn.commit();
	}

	public void close() throws SQLException {
		stmt.close();
		conn.close();
	}
}
