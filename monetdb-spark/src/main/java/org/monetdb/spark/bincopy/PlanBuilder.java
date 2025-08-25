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
import org.monetdb.spark.bincopy.appenders.*;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.steps.*;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Step;

import java.sql.JDBCType;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;

public class PlanBuilder {
	private final HashMap<String, ColumnDescr> schema;
	private final ArrayList<Step> plan;
	private final ArrayList<String> columns;

	public PlanBuilder(ColumnDescr[] tableColumns) {
		schema = new HashMap<>();
		for (var col: tableColumns) {
			schema.put(col.getName(), col);
		}
		plan = new ArrayList<>();
		columns = new ArrayList<>();
	}

	public static Step[] compile(StructField[] sparkTypes, ColumnDescr[] colTypes) throws ConversionError {
		PlanBuilder builder = new PlanBuilder(colTypes);
		builder.plan(sparkTypes);
		return builder.getPlan();

	}

	public void plan(StructType structType) throws ConversionError {
		plan(structType.fields());
	}

	public void plan(StructField[] fields) throws ConversionError {
		for (int i = 0; i < fields.length; i++) {
			StructField field = fields[i];
			String name = field.name();
			ColumnDescr columnDescr = schema.get(name);
			if (columnDescr == null) {
				throw new ConversionError("The table has no column \"" + name + "\"");
			}
			Step[] steps = planOne(i, field, columnDescr);
			Collections.addAll(plan, steps);
			columns.add(name);
		}
	}

	public Step[] getPlan() {
		return plan.toArray(new Step[0]);
	}

	public String[] getColumns() {
		return columns.toArray(new String[0]);
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
	private Step[] planIntegerLikeTypes(int index, StructField sparkField, ColumnDescr columnDescr) throws ConversionError {
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

	private Step[] planStringTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
		final StringExtractor extractor;
		final UTF8StringAppender appender;

		DataType sparkType = sparkField.dataType();
		if (sparkType instanceof StringType || sparkType instanceof VarcharType || sparkType instanceof CharType)
			extractor = new StringExtractor(i);
		else
			return null;

		switch (columnDescr.getType()) {
			case VARCHAR, CLOB, CHAR -> appender = new UTF8StringAppender(i);
			default -> {
				return null;
			}
		}

		return new Step[]{extractor, appender};
	}

	private Step[] planFloatTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
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

	private Step[] planTemporalTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
		DataType sparkType = sparkField.dataType();
		JDBCType colType = columnDescr.getType();
		final Extractor extractor;
		final Appender appender;

		if (sparkType instanceof DateType && colType == JDBCType.DATE) {
			extractor = new IntegerExtractor(i);
			appender = new DateAppender(i);
		} else {
			// regardless of WITH/WITHOUT TIMESTAMP, timestamps come in as a long.
			if (sparkType instanceof TimestampNTZType || sparkType instanceof TimestampType)
				extractor = new LongExtractor(i);
			else
				extractor = null;

			// - If there's no time zone, the timestamp is encoded AS IF the zone
			//   is UTC so the UTCTimestampAppender is appropriate.
			// - If there is a timezone, we can still use the UTCTimestampAppender
			//   because MonetDB always interprets the binary representation as UTC.
			appender = switch (colType) {
				case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> new UTCTimestampAppender(i);
				default -> null;
			};

			if (extractor == null || appender == null)
				return null;
		}

		return new Step[]{extractor, appender};
	}

	private Step[] planBinaryTypes(int i, StructField sparkField, ColumnDescr columnDescr) {
		final Extractor extractor;
		final Appender appender;

		DataType sparkType = sparkField.dataType();
		if (sparkType instanceof BinaryType)
			extractor = new BinaryExtractor(i);
		else
			return null;

		switch (columnDescr.getType()) {
			case BLOB, VARBINARY -> appender = new BinaryAppender(i);
			default -> {
				return null;
			}
		}

		return new Step[]{extractor, appender};
	}

	private Step[] planOne(int i, StructField sparkField, ColumnDescr columnDescr) throws ConversionError {
		Step[] steps;

		steps = planIntegerLikeTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = planStringTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = planFloatTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = planTemporalTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;
		steps = planBinaryTypes(i, sparkField, columnDescr);
		if (steps != null)
			return steps;

		String msg = MessageFormat.format("Field {0} \"{1}\": Don''t know how to convert Spark {2} to SQL type {3}", i, sparkField.name(), sparkField.dataType(), columnDescr);
		throw new ConversionError(msg);
	}
}
