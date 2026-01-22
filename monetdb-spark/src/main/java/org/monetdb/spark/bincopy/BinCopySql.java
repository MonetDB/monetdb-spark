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

import org.monetdb.spark.common.CompressionSettings;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.function.IntFunction;

public class BinCopySql implements Serializable {
	private final String quotedTableName;
	private final String[] columnNames;
	private String identifier;
	private boolean onClient = true;
	private CompressionSettings compression = null;
	private transient IntFunction<String> nameMapper;

	public BinCopySql(String quotedTableName, String[] columnNames) {
		this.quotedTableName = quotedTableName;
		this.columnNames = columnNames.clone();
	}

	public BinCopySql identifier(String identifier) {
		this.identifier = identifier;
		return this;
	}

	public BinCopySql nameMapper(IntFunction<String> f) {
		nameMapper = f;
		return this;
	}

	public BinCopySql onClient(boolean onClient) {
		this.onClient = onClient;
		return this;
	}

	public BinCopySql onServer(boolean onServer) {
		return onClient(!onServer);
	}

	public BinCopySql compression(CompressionSettings settings) {
		this.compression = settings;
		return this;
	}

	public int getColumnCount() {
		return columnNames.length;
	}

	public String getColumnName(int i) {
		return columnNames[i];
	}

	@Override
	public String toString() {
		String sep;

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);

		String identifierComment = identifier != null ? " /* " + identifier.replace("*/", "* /") + " */" : "";
		pw.printf("COPY%s LITTLE ENDIAN BINARY INTO %s (", identifierComment, quotedTableName);
		sep = "";
		for (String col : columnNames) {
			pw.printf("%s\"%s\"", sep, col.replace("\"", "\"\""));
			sep = ", ";
		}
		pw.println(")");

		pw.println("FROM");
		sep = "\t";
		for (int i = 0; i < columnNames.length; i++) {
			String name = nameMapper != null ? nameMapper.apply(i) : Integer.toString(i);
			String escaped = escape(name);
			pw.printf("%s%s", sep, escaped);
			sep = ",\n\t";
		} pw.println();
		pw.print("ON ");
		if (compression != null) {
			pw.print("'" + compression.name() + "' ");
		}
		pw.println(onClient ? "CLIENT" : "SERVER");

		return sw.toString();
	}

	private String escape(String s) {
		if (s.contains("'") || s.contains("\\"))
			return "R'" + s.replace("'", "''") + "'";
		else
			return "'" + s + "'";
	}
}
