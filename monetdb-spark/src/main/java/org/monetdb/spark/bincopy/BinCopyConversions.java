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
import org.monetdb.spark.bincopy.conversions.*;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.common.DecimalSizes;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Converter;

import java.text.MessageFormat;

public class BinCopyConversions {
	public static Converter[] pickConverters(StructField[] fields, ColumnType[] cols) throws ConversionError {
		int n = fields.length;
		if (n != cols.length) {
			throw new ConversionError("Dataframe has " + n + " columns, table has " + cols.length);
		}
		Converter[] converters = new Converter[n];
		for (int i = 0; i < n; i++) {
			DataType fieldType = fields[i].dataType();
			ColumnType colType = cols[i];
			Converter converter = BinCopyConversions.pickConverter(fieldType, colType);
			if (converter == null) {
				throw new ConversionError(MessageFormat.format("Field {0} ({1}): don''t know how to convert Spark type {2} to SQL {3}", i, fields[i].name(), fieldType, colType));
			}
			converters[i] = converter;
		}
		return converters;
	}

	public static Converter pickConverter(DataType fieldType, ColumnType col) {
		switch (col.getType()) {
			case BOOLEAN:
				if (fieldType instanceof BooleanType)
					return new BooleanToBoolean();
				break;
			case TINYINT:
				if (fieldType instanceof ByteType)
					return new ByteToTinyInt();
				break;
			case SMALLINT:
				if (fieldType instanceof ShortType)
					return new ShortToSmallInt();
				break;
			case INTEGER:
				if (fieldType instanceof IntegerType)
					return new IntegerToInteger();
				break;
			case BIGINT:
				if (fieldType instanceof LongType)
					return new LongToBigInt();
				break;
			case FLOAT:
				if (fieldType instanceof FloatType)
					return new FloatToFloat();
				break;
			case DOUBLE:
				if (fieldType instanceof DoubleType)
					return new DoubleToDouble();
				break;
			case CLOB:
			case VARCHAR:
				if (fieldType instanceof StringType)
					return new StringToText();
				break;
			case DECIMAL:
				return pickDecimalConverter(fieldType, col);
			default:
				break;
		}
		return null;
	}

	private static Converter pickDecimalConverter(DataType fieldType, ColumnType col) {
		int precision = col.getPrecision();
		int scale = col.getScale();

		if (!(fieldType instanceof DecimalType decType))
			return null;

		if (decType.precision() > precision) {
			// field is wider, potential overflow
			return null;
		}
		if (decType.scale() > scale) {
			// field is has more decimal digits, potential loss of information
			return null;
		}

		int bits = DecimalSizes.bitsNeededForPrecision(precision);
		return switch (bits) {
			case 8 -> new DecimalToByte();
			case 16 -> new DecimalToShort();
			case 32 -> new DecimalToInteger();
			case 64 -> new DecimalToLong();
			case 128 -> new DecimalToHuge(precision, scale);
			default -> null;
		};
	}
}
