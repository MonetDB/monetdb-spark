package org.monetdb.spark.source;

import java.sql.JDBCType;

public class ColumnType {
	private final JDBCType type;
	private final int precision;
	private final int scale;
	private final String typeName;

	public ColumnType(JDBCType type, int precision, int scale, String typeName) {
		this.type = type;
		this.precision = precision;
		this.scale = scale;
		this.typeName = typeName;
	}

	public ColumnType(JDBCType type) {
		this(type, 0, 0, type.name());
	}

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
	public String toString() {
		if (precision == 0 && scale == 0)
			return typeName;
		else
			return typeName + "(" + precision + "," + scale + ")";
	}
}
