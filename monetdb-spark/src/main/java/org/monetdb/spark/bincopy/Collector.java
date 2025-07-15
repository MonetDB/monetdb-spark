package org.monetdb.spark.bincopy;

import org.apache.commons.io.EndianUtils;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

public class Collector {
	final Conversion[] conversions;
	final ByteArrayOutputStream[] buffers;
	private int rowCount = 0;
	private int totalSize = 0;

	public Collector(StructField[] fields, ColumnType[] cols) throws ConversionError {
		int n = fields.length;
		if (n != cols.length) {
			throw new ConversionError("Dataframe has " + n + " columns, table has " + cols.length);
		}
		conversions = new Conversion[n];
		buffers = new ByteArrayOutputStream[n];
		for (int i = 0; i < n; i++) {
			buffers[i] = new ByteArrayOutputStream();
			DataType fieldType = fields[i].dataType();
			ColumnType colType = cols[i];
			Conversion conv = pickConversion(fieldType, colType);
			if (conv == null) {
				throw new ConversionError(MessageFormat.format("Field {0} ({1}): can''t convert Spark type {2} to {3}", i, fields[i].name(), fieldType, colType));
			}
			conversions[i] = conv;
		}
	}

	public int getRowCount() {
		return rowCount;
	}

	public int getTotalSize() {
		return totalSize;
	}

	public int convertRow(SpecializedGetters row) {
		try {
			for (int i = 0; i < conversions.length; i++) {
				int oldSize = buffers[i].size();
				conversions[i].convert(row, i);
				int newSize = buffers[i].size();
				totalSize += newSize - oldSize;
			}
		} catch (IOException e) {
			// Can't really happen, it's a string buffer, there is no IO
			throw new RuntimeException(e);
		}
		rowCount += 1;
		return totalSize;
	}

	public void clear() {
		for (ByteArrayOutputStream buffer : buffers) {
			buffer.reset();
		}
		rowCount = 0;
		totalSize = 0;
	}

	public String copyStatement(String quotedTableName) {
		String buf = "COPY LITTLE ENDIAN BINARY INTO " + quotedTableName + " FROM ";
		String sep = "";
		for (int i = 0; i < buffers.length; i++) {
			buf += sep + "'" + i + "'";
			sep = ", ";
		}
		buf += " ON CLIENT";
		return buf;
	}

	public void writeCollected(int idx, OutputStream dest) throws IOException {
		buffers[idx].writeTo(dest);
	}

	private Conversion pickConversion(DataType fieldType, ColumnType col) {
		switch (col.getType()) {
			case BOOLEAN:
				if (fieldType instanceof BooleanType)
					return this::booleanFromBoolean;
				break;
			case INTEGER:
				if (fieldType instanceof IntegerType)
					return this::integerFromInteger;
				break;
			case DOUBLE:
				if (fieldType instanceof DoubleType)
					return this::doubleFromDouble;
				break;
			case CLOB:
			case VARCHAR:
				if (fieldType instanceof StringType)
					return this::textFromString;
				break;
			default:
				break;
		}
		return null;
	}

	private void booleanFromBoolean(SpecializedGetters row, int idx) {
		boolean b = row.getBoolean(idx);
		int numeric = b ? 1 : 0;
		buffers[idx].write(numeric);
	}

	private void integerFromInteger(SpecializedGetters row, int idx) throws IOException {
		int i = row.getInt(idx);
		EndianUtils.writeSwappedInteger(buffers[idx], i);
	}

	private void doubleFromDouble(SpecializedGetters row, int idx) throws IOException {
		double d = row.getDouble(idx);
		EndianUtils.writeSwappedDouble(buffers[idx], d);
	}

	private void textFromString(SpecializedGetters row, int idx) throws IOException {
		ByteArrayOutputStream s = buffers[idx];
		UTF8String u = row.getUTF8String(idx);
		u.writeTo(s);
		s.write(0);
	}

	@FunctionalInterface
	interface Conversion {
		void convert(SpecializedGetters row, int idx) throws IOException;
	}
}
