package org.monetdb.spark;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import scala.Option;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

public class MonetDialect extends JdbcDialect {
	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:monetdb:") || url.startsWith("jdbc:monetdbs:");
	}


	@Override
	public Option<JdbcType> getJDBCType(DataType dt) {
		JdbcType type = null;
		if (dt instanceof BooleanType) {
			type = new JdbcType("BOOLEAN", Types.BOOLEAN);
		} else if (dt instanceof ByteType) {
			// map into larger type because MonetDB uses the most negative to repreent NULL
			type = new JdbcType("SMALLINT", Types.SMALLINT);
		} else if (dt instanceof ShortType) {
			// map into larger type
			type = new JdbcType("INTEGER", Types.INTEGER);
		} else if (dt instanceof IntegerType) {
			//  map into larger type
			type = new JdbcType("BIGINT", Types.BIGINT);
		} else if (dt instanceof LongType) {
			// do NOT map into larger type, try to avoid HUGEINT
			// because there is no Spark equivalent to read back into.
			// (Is this wise? we now have data loss on -9223372036854775808.)
			type = new JdbcType("BIGINT", Types.BIGINT);
		} else if (dt instanceof TimestampType) {
			// In Spark, TimestampType is time zone aware
			type = new JdbcType("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE);
		} else if (dt instanceof TimestampType) {
			// In Spark, TimestampNTZType is not time zone aware
			type = new JdbcType("TIMESTAMP", Types.TIMESTAMP);
		}

		Option<JdbcType> ret = type != null ? Option.apply(type) : super.getJDBCType(dt);
		return ret;
	}

	@Override
	public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
		DataType type = null;
		JDBCType jdbcType = JDBCType.valueOf(sqlType);
		switch (jdbcType) {
			case TINYINT -> type = DataTypes.ByteType;
			case SMALLINT -> type = DataTypes.ShortType;
			case INTEGER -> {
				final byte YEAR = 0;
				final byte MONTH = 1;
				switch (typeName) {
					case "interval year" -> type = DataTypes.createYearMonthIntervalType(YEAR, YEAR);
					case "interval year to month" -> type = DataTypes.createYearMonthIntervalType(YEAR, MONTH);
					case "interval month" -> type = DataTypes.createYearMonthIntervalType(MONTH, MONTH);
				}
			}
			case DECIMAL, NUMERIC -> {
				final byte DAY = 0;
				final byte HOUR = 1;
				final byte MINUTE = 2;
				final byte SECOND = 3;
				switch (typeName) {
					case "interval day" -> type = DataTypes.createDayTimeIntervalType(DAY, DAY);
					case "interval day to hour" -> type = DataTypes.createDayTimeIntervalType(DAY, HOUR);
					case "interval day to minute" -> type = DataTypes.createDayTimeIntervalType(DAY, MINUTE);
					case "interval day to second" -> type = DataTypes.createDayTimeIntervalType(DAY, SECOND);
					case "interval hour" -> type = DataTypes.createDayTimeIntervalType(HOUR, HOUR);
					case "interval hour to minute" -> type = DataTypes.createDayTimeIntervalType(HOUR, MINUTE);
					case "interval hour to second" -> type = DataTypes.createDayTimeIntervalType(HOUR, SECOND);
					case "interval minute" -> type = DataTypes.createDayTimeIntervalType(MINUTE, MINUTE);
					case "interval minute to second" -> type = DataTypes.createDayTimeIntervalType(MINUTE, SECOND);
					case "interval second" -> type = DataTypes.createDayTimeIntervalType(SECOND, SECOND);
				}
			}
			case REAL ->
					type = DataTypes.FloatType; // is this always safe? or can funny precision switch to DoubleType?
			case TIMESTAMP_WITH_TIMEZONE -> type = DataTypes.TimestampType;
			case TIMESTAMP -> type = DataTypes.TimestampNTZType;
		}

		if (type == null)
			return super.getCatalystType(sqlType, typeName, size, md);
		else
			return Option.apply(type);
	}

	@Override
	public LocalDateTime convertJavaTimestampToTimestampNTZ(Timestamp t) {
		// Unfortunately, Spark has already decided to retrieve the timestamp from JDBC as a
		// java.sql.Timestamp. That involves an enormous amount of unnecessary
		// work to determine a time zone object which we will not use.
		// It also holds correct year, hour, etc fields.
		// Calling .toLocalDateTime extracts these.
		//
		// The superclass implementation did something much more complicated
		// which also turned out to give wrong results.
		//
		// It would have been much better if Spark had given us a String,
		// or even better, given us the ResultSet object so we could have picked
		// the most efficient way to extract it.
		//
		// Maybe the JIT compiler will notice we're not using the time zone object
		// and elide its construction. But I'm not holding my breath.
		LocalDateTime ldt = t.toLocalDateTime();
		return ldt;
	}

	@Override
	public Timestamp convertJavaTimestampToTimestamp(Timestamp t) {
		// is this correct?
		return t;
	}

	@Override
	public Timestamp convertTimestampNTZToJavaTimestamp(LocalDateTime ldt) {
		// is this correct?
		return Timestamp.valueOf(ldt);
	}

	@Override
	public int getYearMonthIntervalAsMonths(String yearmonthStr) {
		return Integer.parseInt(yearmonthStr);
	}

	@Override
	public long getDayTimeIntervalAsMicros(String daytimeStr) {
		// comes in as a 3 digit decimal
		int n = daytimeStr.length();
		int dot = daytimeStr.length() - 4; // three digits and a '.'
		String whole = daytimeStr.substring(0, dot);
		String fraction = daytimeStr.substring(dot + 1);

		long wholeSeconds = Long.parseLong(whole, 10);
		long fractionMillis = Long.parseLong(fraction, 10);
		// Careful, wholeSeconds may be negative
		long sign1000 = wholeSeconds < 0 ? -1000L : 1000L;
		return 1000_000L * wholeSeconds + sign1000 * fractionMillis;
	}
}
