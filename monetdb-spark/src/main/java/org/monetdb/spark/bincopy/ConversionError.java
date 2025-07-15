package org.monetdb.spark.bincopy;

public class ConversionError extends Exception {
	public ConversionError(String message) {
		super(message);
	}

	public ConversionError(String message, Throwable cause) {
		super(message, cause);
	}
}
