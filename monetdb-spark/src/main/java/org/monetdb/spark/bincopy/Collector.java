package org.monetdb.spark.bincopy;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.Extractor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Collector {
	final Extractor[] extractors;
	final ByteArrayOutputStream[] buffers;
	private int rowCount = 0;
	private int totalSize = 0;

	public Collector(Extractor[] extractors) throws ConversionError {
		this.extractors = extractors;
		buffers = new ByteArrayOutputStream[extractors.length];
		for (int i = 0; i < extractors.length; i++) {
			buffers[i] = new ByteArrayOutputStream();
		}
		for (int i = 0; i < extractors.length; i++) {
			extractors[i].init(this, i);
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

}
