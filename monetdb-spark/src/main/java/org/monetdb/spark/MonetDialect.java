package org.monetdb.spark;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import scala.Option;

import java.sql.JDBCType;
import java.sql.Types;

public class MonetDialect extends JdbcDialect {
	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:monetdb:") || url.startsWith("jdbc:monetdbs:");
	}

	@Override
	public Option<JdbcType> getJDBCType(DataType dt) {
		if (dt instanceof BooleanType) {
			JdbcType t = new JdbcType("BOOLEAN", Types.BOOLEAN);
			return Option.apply(t);
		} else if (dt instanceof ByteType) {
			JdbcType t = new JdbcType("TINYINT", Types.TINYINT);
			return Option.apply(t);
		} else if (dt instanceof ShortType) {
			JdbcType t = new JdbcType("SMALLINT", Types.SMALLINT);
			return Option.apply(t);
		}
		Option<JdbcType> t = super.getJDBCType(dt);
		return t;
	}

	@Override
	public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
		DataType type;
		switch (JDBCType.valueOf(sqlType)) {
			case SMALLINT -> type = DataTypes.ShortType;
			default -> {
				return super.getCatalystType(sqlType, typeName, size, md);
			}
		}
		return Option.apply(type);
	}
}
