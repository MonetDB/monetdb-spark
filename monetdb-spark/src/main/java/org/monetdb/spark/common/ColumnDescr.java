/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.common;

import java.io.Serial;
import java.io.Serializable;
import java.sql.JDBCType;
import java.util.Objects;

public record ColumnDescr(String name, JDBCType type, int precision, int scale, String typeName) implements Serializable {
	@Serial
	private static final long serialVersionUID = 0L;

	public ColumnDescr(String name, JDBCType type) {
		this(name, type, 0, 0, type.name());
	}


	@Override
	public boolean equals(Object o) {
		if (o == null || getClass() != o.getClass())
			return false;
		ColumnDescr that = (ColumnDescr) o;
		return type == that.type && precision == that.precision && scale == that.scale;
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, precision, scale);
	}

	@Override
	public String toString() {
		if (precision == 0 && scale == 0)
			return typeName;
		else
			return typeName + "(" + precision + "," + scale + ")";
	}
}
