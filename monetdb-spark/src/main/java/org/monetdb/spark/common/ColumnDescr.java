/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.common;

import java.io.Serial;
import java.io.Serializable;
import java.sql.JDBCType;
import java.util.Objects;

public final class ColumnDescr implements Serializable {
	@Serial
	private static final long serialVersionUID = 0L;
	private final String name;
	private final JDBCType type;
	private final int precision;
	private final int scale;
	private final String typeName;

	public ColumnDescr(String name, JDBCType type, int precision, int scale, String typeName) {
		this.name = name;
		this.type = type;
		this.precision = precision;
		this.scale = scale;
		this.typeName = typeName;
	}

	public ColumnDescr(JDBCType type) {
		this(null, type, 0, 0, type.name());
	}

	public String getName() { return name; }

	public JDBCType getType() {
		return type;
	}

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public String getTypeName() {
		return typeName;
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
