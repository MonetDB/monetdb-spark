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

import org.apache.spark.sql.types.*;
import org.monetdb.spark.bincopy.appenders.Appender;
import org.monetdb.spark.bincopy.appenders.DoubleAppender;
import org.monetdb.spark.bincopy.appenders.FloatAppender;
import org.monetdb.spark.bincopy.appenders.UTF8StringAppender;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.steps.*;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Step;

import java.sql.JDBCType;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;

public class PlanCompiler {
	public static Step[] compile(StructField[] fields, ColumnDescr[] cols) throws ConversionError {
		int n = fields.length;
		if (n != cols.length) {
			throw new ConversionError("Dataframe has " + n + " columns, table has " + cols.length);
		}

		ArrayList<Step> res = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			var sub = compileOne(i, fields[i], cols[i]);
			Collections.addAll(res, sub);
		}

		return res.toArray(new Step[0]);
	}

	public static Step[] compileOne(int i, StructField sparkField, ColumnDescr columnDescr) throws ConversionError {
		Step[] steps;

		steps = compileIntegerLikeTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = compileStringTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = compileFloatTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;

		String msg = MessageFormat.format("Field {0} \"{1}\": Don''t know how to convert Spark {2} to SQL type {3}", i, sparkField.name(), sparkField.dataType(), columnDescr);
		throw new ConversionError(msg);
	}

	/**
	 * Compile conversions between various integer sizes, boolean and
	 * DECIMAL(x,y) for x up to 18.
	 *
	 * @param index       Index of the item being converted in the list of fields and the list of columns
	 * @param sparkField  Spark field being read from
	 * @param columnDescr Column being written to
	 * @return the Steps that must be executed to convert one field.
	 * @throws ConversionError
	 */
	private static Step[] compileIntegerLikeTypes(int index, StructField sparkField, ColumnDescr columnDescr) throws ConversionError {
		DataType sparkType = sparkField.dataType();

		// Are they at least somewhat integer-shaped?
		SizeRange src = new SizeRange(sparkType);
		SizeRange dst = new SizeRange(columnDescr);
		if (!src.valid() || !dst.valid())
			return null;

		// The scales must match
		int sparkScale = sparkType instanceof DecimalType dec ? dec.scale() : 0;
		if (sparkScale != columnDescr.getScale())
			return null;

		Extractor extractor = src.getExtractor(index);
		Appender appender = dst.getAppender(index);

		boolean srcHuge = src.size > 64;
		boolean dstHuge = dst.size > 64;
		ArrayList<Step> steps = new ArrayList<>();
		steps.add(extractor);

		// Decide on the range check.
		final Step rangeCheck;
		final Step conversion;
		if (srcHuge) {
			if (dstHuge) {
				// They must both be decimals so SizeRange.precision is set.
				if (dst.decimalPrecision >= src.decimalPrecision)
					// no check needed
					rangeCheck = null;
				else {
					rangeCheck = new BigIntRangeCheck(dst.decimalPrecision);
				}
				conversion = null;
			} else {
				// src is huge (decimal) and dst is not
				rangeCheck = new BigIntRangeCheck(dst.range);
				conversion = new BigIntToLongConversion();
			}
		} else {
			if (dstHuge) {
				// src is not huge and dst is huge. It will certainly fit.
				rangeCheck = null;
				conversion = new LongToBigIntConversion();
			} else {
				// neither is huge so it depends on the ranges
				if (dst.range.contains(src.range)) {
					rangeCheck = null;
				} else {
					rangeCheck = new LongRangeCheck(dst.range);
				}
				conversion = null;
			}
		}
		if (rangeCheck != null)
			steps.add(rangeCheck);
		if (conversion != null)
			steps.add(conversion);
		steps.add(appender);

		return steps.toArray(new Step[0]);
	}

	private static Step[] compileStringTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
		switch (columnDescr.getType()) {
			case VARCHAR:
			case CLOB:
				if (sparkField.dataType() instanceof StringType) {
					StringExtractor extractor = new StringExtractor(i);
					UTF8StringAppender appender = new UTF8StringAppender(i);
					return new Step[]{extractor, appender};
				}
				break;
		}
		return null;
	}


	private static Step[] compileFloatTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
		DataType sparkType = sparkField.dataType();
		JDBCType colType = columnDescr.getType();

		Extractor extractor //
				= sparkType instanceof FloatType ? new FloatExtractor(i) //
				: sparkType instanceof DoubleType ? new DoubleExtractor(i) //
				: null;

		Appender appender = switch (colType) {
			case REAL -> new FloatAppender(i);
			case DOUBLE -> new DoubleAppender(i);
			default -> null;
		};

		if (extractor == null || appender == null)
			return null;

		return new Step[]{extractor, new FloatRangeCheck(), appender};
	}
}
