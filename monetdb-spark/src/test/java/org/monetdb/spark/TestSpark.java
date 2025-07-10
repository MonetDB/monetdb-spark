package org.monetdb.spark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Test Spark itself, to get to know it
 */
public class TestSpark {
	@BeforeAll
	public static void checkMonetDBReachable() throws SQLException {
		String url = Config.dbUrl();
		try (Connection conn = DriverManager.getConnection(url)) {
			conn.getMetaData().getDriverVersion();
		}
	}

	@Test
	public void testFoo() {}
}