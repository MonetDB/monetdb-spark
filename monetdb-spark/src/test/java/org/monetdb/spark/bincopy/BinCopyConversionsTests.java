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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.monetdb.spark.MockRow;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Step;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.JDBCType;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinCopyConversionsTests {
	final StructField boolField = new StructField("b", DataTypes.BooleanType, false, Metadata.empty());
	final StructField intField = new StructField("i", DataTypes.IntegerType, false, Metadata.empty());
	final StructField stringField = new StructField("t", DataTypes.StringType, false, Metadata.empty());
	final ColumnDescr boolCol = new ColumnDescr("b", JDBCType.BOOLEAN);
	final ColumnDescr intCol = new ColumnDescr("i", JDBCType.INTEGER);
	final ColumnDescr stringCol = new ColumnDescr("t", JDBCType.VARCHAR);

	Collector col;
	private Step[] steps;

	@Test
	public void testLengthMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnDescr[] colTypes = {boolCol, intCol};
		PlanBuilder builder = new PlanBuilder(colTypes);
		assertThrows(ConversionError.class, () -> builder.plan(sparkTypes));
	}

	@Test
	public void testTypeMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnDescr[] colTypes = {boolCol, stringCol, stringCol};
		PlanBuilder builder = new PlanBuilder(colTypes);
		assertThrows(ConversionError.class, () -> builder.plan(sparkTypes));
	}

	private String formatCollected(int idx) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		col.getBuffer(idx).writeTo(out);
		byte[] bytes = out.toByteArray();
		StringBuilder buf = new StringBuilder();
		for (byte b : bytes) {
			if (b == '$') {
				buf.append("$$");
			} else if (b >= ' ' && b < 127) {
				buf.append((char) b);
			} else {
				final String hex = "0123456789abcdef";
				char hi = hex.charAt(b / 16);
				char lo = hex.charAt(b % 16);
				buf.append('$');
				buf.append(hi);
				buf.append(lo);
			}
		}
		return buf.toString();
	}

	@Test
	public void testCollector() throws ConversionError, IOException {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnDescr[] colTypes = {boolCol, intCol, stringCol};
		PlanBuilder builder = new PlanBuilder(colTypes);
		builder.plan(sparkTypes);

		steps = builder.getPlan();
		col = new Collector();
		col.registerWithConverters(steps);

		assertEquals("", formatCollected(0));
		assertEquals("", formatCollected(1));
		assertEquals("", formatCollected(2));

		MockRow row = new MockRow(TRUE, 1, "one");
		convertRow(row);

		row = new MockRow(FALSE, 2, "two");
		convertRow(row);

		row = new MockRow(TRUE, 3, "three");
		convertRow(row);

		// booleans are a single byte
		assertEquals("$01$00$01", formatCollected(0));
		// integers are 4 bytes, little endian
		assertEquals("$01$00$00$00$02$00$00$00$03$00$00$00", formatCollected(1));
		// strings are NUL separated
		assertEquals("one$00two$00three$00", formatCollected(2));

		assertEquals(3, col.getRowCount());
		int col1Size = 3; // one byte per row
		int col2Size = 12; // four bytes per row
		int col3Size = 3 + 1 + 3 + 1 + 5 + 1;
		assertEquals(col1Size + col2Size + col3Size, col.getTotalSize());
	}

	private void convertRow(MockRow row) throws IOException {
		for (Step step : steps)
			step.exec(row);
		col.endRow();
	}
}