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

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

public class BinCopySql implements Serializable {
	String quotedTableName;
	String[] columnNames;
	String identifier;
	String prefix = "";
	String suffix = "";
	boolean onClient = true;

	public BinCopySql(String quotedTableName, String[] columnNames) {
		this.quotedTableName = quotedTableName;
		this.columnNames = columnNames.clone();
	}

	public BinCopySql identifier(String identifier) {
		this.identifier = identifier;
		return this;
	}

	public BinCopySql suffix(String suffix) {
		this.suffix = suffix;
		return this;
	}

	public BinCopySql prefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	public BinCopySql onClient(boolean onClient) {
		this.onClient = onClient;
		return this;
	}

	public BinCopySql onServer(boolean onServer) {
		return onClient(!onServer);
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

		pw.print("FROM ");
		final String qprefix, qsuffix;
		if (prefix.contains("\\") || prefix.contains("'") || suffix.contains("\\") || suffix.contains("'")) {
			qprefix = "R'" + prefix.replace("'", "''");
			qsuffix = suffix.replace("'", "''") + "'";
		} else {
			qprefix = "'" + prefix;
			qsuffix = suffix + "'";
		}
		sep = "";
		for (int i = 0; i < columnNames.length; i++) {
			pw.printf("%s%s%d%s", sep, qprefix, i, qsuffix);
			sep = ", ";
		} pw.println();
		pw.println(onClient ? "ON CLIENT" : "ON SERVER");

		return sw.toString();
	}
}
