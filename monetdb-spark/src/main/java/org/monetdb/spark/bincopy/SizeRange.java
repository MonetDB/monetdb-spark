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
import org.monetdb.spark.bincopy.appenders.Int128Appender;
import org.monetdb.spark.bincopy.appenders.IntegerLikeAppender;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.Range;
import org.monetdb.spark.common.steps.BooleanExtractor;
import org.monetdb.spark.common.steps.Extractor;
import org.monetdb.spark.common.steps.HugeDecimalExtractor;
import org.monetdb.spark.common.steps.IntLikeExtractor;
import org.monetdb.spark.workerside.ConversionError;

import java.sql.JDBCType;

import static org.monetdb.spark.common.DecimalSizes.bitsNeededForPrecision;

/**
 * Centralize size- and range logic for types that can be stored in a Long.
 * <p>
 * If valid() returns true, the size is 8, 16, 32, 64 or 128 depending on
 * how many bits are needed to hold the type. Range is the range of the type
 * if it fits in a Long. If the range is too large to fit in a long, range
 * is set to null.
 */
public class SizeRange {
	public final int size;
	public final Range range;
	public final boolean isBool;
	public final int decimalPrecision;
	public final int decimalScale;

	public SizeRange(ColumnDescr columnDescr) {
		JDBCType jdbcType = columnDescr.getType();
		int precision = columnDescr.getPrecision();

		isBool = jdbcType == JDBCType.BOOLEAN;

		size = switch (jdbcType) {
			case TINYINT, BOOLEAN -> 8;
			case SMALLINT -> 16;
			case INTEGER -> 32;
			case BIGINT -> 64;
			case DECIMAL, NUMERIC -> bitsNeededForPrecision(precision);
			default -> -1;
		};

		range = switch (jdbcType) {
			case BOOLEAN -> new Range(0, 1);
			case DECIMAL, NUMERIC -> size > 64 ? null : Range.forDecimalWidth(precision);
			default -> size < 0 ? null : Range.forIntegerSymmetric(size);
		};

		if (jdbcType == JDBCType.DECIMAL || jdbcType == JDBCType.NUMERIC) {
			decimalPrecision = precision;
			decimalScale = columnDescr.getScale();
		} else {
			decimalPrecision = -1;
			decimalScale = -1;
		}
	}

	public SizeRange(DataType sparkType) {
		isBool = sparkType instanceof BooleanType;

		size = isBool ? 8 //
				: sparkType instanceof ByteType ? 8 //
				: sparkType instanceof ShortType ? 16 //
				: sparkType instanceof IntegerType ? 32 //
				: sparkType instanceof LongType ? 64 //
				: sparkType instanceof DecimalType dec ? bitsNeededForPrecision(dec.precision()) //
				: -1;

		if (size < 0 || size > 64)
			range = null;
		else if (isBool)
			range = new Range(0, 1);
		else if (sparkType instanceof DecimalType dec)
			range = Range.forDecimalWidth(dec.precision());
		else
			range = Range.forIntegerFull(size);

		if (sparkType instanceof DecimalType dec) {
			decimalPrecision = dec.precision();
			decimalScale = dec.scale();
		} else {
			decimalPrecision = -1;
			decimalScale = -1;
		}
	}

	public boolean valid() {
		return size > 0;
	}

	public Extractor getExtractor(int index) throws ConversionError {
		if (isBool)
			return new BooleanExtractor(index);
		else if (size == 128 && decimalPrecision > 0) {
			return new HugeDecimalExtractor(index, decimalPrecision, decimalScale);
		} else
			return IntLikeExtractor.forSize(index, size);
	}

	public Appender getAppender(int index) {
		if (size == 128)
			return new Int128Appender(index);
		else
			return IntegerLikeAppender.forIntSize(index, size);
	}
}
