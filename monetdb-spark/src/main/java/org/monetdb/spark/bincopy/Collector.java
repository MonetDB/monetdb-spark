package org.monetdb.spark.bincopy;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.types.*;
import org.monetdb.spark.common.ColumnType;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Extractor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

public class Collector {
	final Extractor[] extractors;
	final ByteArrayOutputStream[] buffers;
	private int rowCount = 0;
	private int totalSize = 0;

	public Collector(StructField[] fields, ColumnType[] cols) throws ConversionError {
		int n = fields.length;
		if (n != cols.length) {
			throw new ConversionError("Dataframe has " + n + " columns, table has " + cols.length);
		}
		extractors = new Extractor[n];
		buffers = new ByteArrayOutputStream[n];
		for (int i = 0; i < n; i++) {
			buffers[i] = new ByteArrayOutputStream();
			DataType fieldType = fields[i].dataType();
			ColumnType colType = cols[i];
			Extractor extractor = pickExtractor(fieldType, colType);
			if (extractor == null) {
				throw new ConversionError(MessageFormat.format("Field {0} ({1}): can''t convert Spark type {2} to {3}", i, fields[i].name(), fieldType, colType));
			}
			extractor.init(this, i);
			extractors[i] = extractor;
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
			for (int i = 0; i < extractors.length; i++) {
				int oldSize = buffers[i].size();
				extractors[i].extract(row, i);
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

	private Extractor pickExtractor(DataType fieldType, ColumnType col) {
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
			default:
				break;
		}
		return null;
	}
}
