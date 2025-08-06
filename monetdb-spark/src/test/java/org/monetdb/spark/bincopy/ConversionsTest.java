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
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.workerside.ConversionError;

import java.sql.JDBCType;

import static org.junit.jupiter.api.Assertions.*;

class ConversionsTest {
	final StructField boolField = new StructField("b", DataTypes.BooleanType, false, Metadata.empty());
	final StructField intField = new StructField("i", DataTypes.IntegerType, false, Metadata.empty());
	final StructField stringField = new StructField("s", DataTypes.StringType, false, Metadata.empty());
	final ColumnType boolCol = new ColumnType(JDBCType.BOOLEAN);
	final ColumnType intCol = new ColumnType(JDBCType.INTEGER);
	final ColumnType stringCol = new ColumnType(JDBCType.VARCHAR);

	Collector col;

	@Test
	public void testLengthMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {boolCol, intCol};
		assertThrows(ConversionError.class, () -> Conversions.pickExtractors(sparkTypes, colTypes))
		;
	}

	@Test
	public void testTypeMismatch() {
		StructField[] sparkTypes = {boolField, intField, stringField};
		ColumnType[] colTypes = {boolCol, stringCol, stringCol};
		assertThrows(ConversionError.class, () -> Conversions.pickExtractors(sparkTypes, colTypes));
	}

}