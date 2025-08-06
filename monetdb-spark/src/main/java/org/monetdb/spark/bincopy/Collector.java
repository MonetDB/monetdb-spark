package org.monetdb.spark.bincopy;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Collector {
	final Converter[] converters;
	final ByteArrayOutputStream[] buffers;
	private int rowCount = 0;
	private int totalSize = 0;

	public Collector(Converter[] converters) throws ConversionError {
		this.converters = converters;
		buffers = new ByteArrayOutputStream[converters.length];
		for (int i = 0; i < converters.length; i++) {
			buffers[i] = new ByteArrayOutputStream();
		}
		for (int i = 0; i < converters.length; i++) {
			converters[i].init(this, i);
		}
	}

	public ByteArrayOutputStream getBuffer(int idx) {
		return buffers[idx];
	}

	public int getRowCount() {
		return rowCount;
	}

	public int getTotalSize() {
		return totalSize;
	}

	public int convertRow(SpecializedGetters row) {
		try {
			for (int i = 0; i < converters.length; i++) {
				int oldSize = buffers[i].size();
				converters[i].extract(row, i);
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

}
