package org.monetdb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Spark itself, to get to know it
 */
public class TestsToLearnSpark {
	@AutoClose
	Connection conn = null;
	@AutoClose
	Statement stmt = null;
	@AutoClose
	SparkSession spark = null;

	@BeforeAll
	public static void checkMonetDBReachable() throws SQLException {
		Config.checkDatabase();
	}

	public void connect() throws SQLException {
		conn = Config.connectDatabase();
		stmt = conn.createStatement();
	}

	void spark() {
		spark = Config.sparkSession();
	}

	@Test
	public void testRoundtrip() throws SQLException {
		int n = 10;
		connect();
		stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo(id INT, b BOOLEAN, t TEXT)");

		spark();
		Dataset<Long> spine = spark.range(n);
		Dataset<Row> df = spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("t", concat(lit("x"), col("id")));

		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.option("truncate", true)
				.save();

		ResultSet rs = stmt.executeQuery("SELECT id, b, t FROM foo ORDER BY id");
		for (int i = 0; i < n; i++) {
			assertTrue(rs.next());
			assertEquals(i, rs.getInt(1));
			assertEquals(i % 2 == 0, rs.getBoolean(2));
			assertEquals("x" + i, rs.getString(3));
		}
		assertFalse(rs.next());
	}

	@Test
	public void testOverwrite() throws SQLException {
		int n = 10;
		connect();
		stmt.execute("DROP TABLE IF EXISTS foo");

		spark();

		Dataset<Long> spine = spark.range(n);
		Dataset<Row> df = spine
				.withColumn("b", col("id").mod(2).equalTo(0))
				.withColumn("t", concat(lit("x"), col("id")));

		df
				.write()
				.format("jdbc")
				.mode(SaveMode.Overwrite)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo")
				.option("truncate", true)
				.option("numPartitions", 1)
				.save();

		ResultSet rs = stmt.executeQuery("SELECT id, b, t FROM foo ORDER BY id");
		for (int i = 0; i < n; i++) {
			assertTrue(rs.next());
			assertEquals(i, rs.getInt(1));
			assertEquals(i % 2 == 0, rs.getBoolean(2));
			assertEquals("x" + i, rs.getString(3));
		}
		assertFalse(rs.next());
	}

	@Test
	public void testUTF8String() throws IOException {
		String str = "AAP";
		byte[] aapBytes = {0x41, 0x41, 0x50};
		assertArrayEquals(aapBytes, str.getBytes(StandardCharsets.UTF_8));
		UTF8String utf8str = UTF8String.fromString(str);
		byte[] ubytes = utf8str.getBytes();
		assertArrayEquals(aapBytes, ubytes);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		utf8str.writeTo(out);
		byte[] sbytes = out.toByteArray();
		assertArrayEquals(aapBytes, sbytes);
	}

	@Test
	public void testDecimal() {
		Decimal dec;

		// Fits a long
		dec = Decimal.apply(1234, 8, 2);
		assertEquals("12.34", dec.toString());
		assertEquals(1234, dec.toUnscaledLong());
		assertEquals(8, dec.precision());
		assertEquals(2, dec.scale());

		dec = Decimal.apply("12.34");
		assertEquals("12.34", dec.toString());
		assertEquals(1234, dec.toUnscaledLong());
		assertEquals(4, dec.precision()); // fromString sets the minimum required precision
		assertEquals(2, dec.scale());

		Decimal ten = Decimal.apply(10);
		Decimal dec10 = dec.$times(ten);
		assertEquals("12.34", dec.toString());
		assertEquals("123.40", dec10.toString());

		Decimal big = dec;
		for (int i = 0; i < 20; i++)
			big = big.$times(ten);
		assertEquals("1234000000000000000000.00", big.toString());

		// big is too big to fit in a log
		final var big2 = big;
		assertThrows(ArithmeticException.class, () -> System.err.println(big2.toUnscaledLong()));
		// but we can get the underlying biginteger
		BigInteger bi = big.toJavaBigDecimal().unscaledValue();
		// it's unscaled, so basically the decimal value with the dot stripped
		assertEquals(big.toString().replace(".", ""), bi.toString());
	}
}