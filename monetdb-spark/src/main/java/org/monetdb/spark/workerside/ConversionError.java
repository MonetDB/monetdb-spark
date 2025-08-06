/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.workerside;

public class ConversionError extends Exception {
	public ConversionError(String message) {
		super(message);
	}

	public ConversionError(String message, Throwable cause) {
		super(message, cause);
	}
}
