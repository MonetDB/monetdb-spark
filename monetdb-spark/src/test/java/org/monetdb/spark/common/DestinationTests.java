/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.common;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.monetdb.mcl.net.Parameter;
import org.monetdb.mcl.net.Target;
import org.monetdb.mcl.net.ValidationError;
import org.monetdb.spark.Config;

import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class DestinationTests {

	private final static String USER = "sparky";
	private final static String PASSWORD = "100%spark";
	private final static String SCHEMA = "awk\" \"ward";
	private final static String TABLE = "mydata";

	private final ColumnDescr[] QUALIFIED_TABLE_TYPE = new ColumnDescr[]{ //
			new ColumnDescr(JDBCType.BOOLEAN, 1, 0, "BOOL"), //
			new ColumnDescr(JDBCType.TINYINT, 3, 0, "TINYINT"), //
			new ColumnDescr(JDBCType.INTEGER, 10, 0, "INTEGER"), //
			new ColumnDescr(JDBCType.VARCHAR, 8, 0, "VARCHAR"), //
			new ColumnDescr(JDBCType.VARCHAR, 0, 0, "CLOB"), //
	};

	private final ColumnDescr[] SYS_TABLE_TYPE = new ColumnDescr[]{new ColumnDescr(JDBCType.INTEGER, 10, 0, "INTEGER"),};

	private static String quoteId(String id) {
		return "\"" + id.replace("\"", "\"\"") + "\"";
	}

	private static String quoteStr(String s) {
		return "R'" + s.replace("'", "''") + "'";
	}

	private static String userUrl(boolean includeCredentials) throws URISyntaxException, ValidationError {
		String baseUrl = Config.databaseUrl();
		Target target = new Target(baseUrl, null);
		// target.buildUrl automatically omits the credentials, we clear them anyway just to be sure
		target.clear(Parameter.USER);
		target.clear(Parameter.PASSWORD);
		String url = target.buildUrl();
		if (includeCredentials) {
			// quick and dirty
			url += url.contains("?") ? "&" : "?";
			url += "user=";
			url += URLEncoder.encode(USER, StandardCharsets.UTF_8);
			url += "&password=";
			url += URLEncoder.encode(PASSWORD, StandardCharsets.UTF_8);
		}
		return url;
	}

	@BeforeAll
	public static void setUpDatabase() throws SQLException, URISyntaxException, ValidationError {
		String qqUser = quoteId(USER);
		String qPassword = quoteStr(PASSWORD);
		String qqSchema = quoteId(SCHEMA);
		String qqTable = quoteId(TABLE);

		// Log in as root to create user and schema
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			// Drop preexisting schema, user and sys table
			stmt.execute("DROP TABLE IF EXISTS sys." + qqTable);
			try {
				stmt.execute("DROP SCHEMA " + qqSchema + " CASCADE");
			} catch (SQLException e) {
				if (!e.getMessage().contains("does not exist"))
					throw e;
			}
			try {
				stmt.execute("DROP USER " + qqUser);
			} catch (SQLException e) {
				if (!e.getMessage().contains("no such user"))
					throw e;
			}
			// Create user with schema 'sys' so it's easier to drop the user later on
			stmt.execute("CREATE USER " + qqUser + " WITH UNENCRYPTED PASSWORD " + qPassword + " NAME 'Sparky Spark' SCHEMA sys");
			stmt.execute("CREATE SCHEMA " + qqSchema + " AUTHORIZATION " + qqUser);

			// Create a 'decoy' table
			stmt.execute("CREATE TABLE sys." + qqTable + "(dummy INT)");
			stmt.execute("GRANT SELECT ON sys." + qqTable + " TO " + qqUser);
		}

		// Log in as user to create the table
		String url = userUrl(true);
		try (Connection conn = DriverManager.getConnection(url); Statement stmt = conn.createStatement()) {
			String qqFullName = qqSchema + "." + qqTable;
			stmt.execute("CREATE TABLE " + qqFullName + "(b BOOLEAN, i8 TINYINT, i32 INT, v VARCHAR(8), t TEXT)");
		}
	}

	private String testLogin(String url, String user, String password) throws SQLException {
		Destination dest = new Destination(url, user, password, TABLE);
		try (Connection conn = dest.connect()) {
			return conn.getMetaData().getUserName();
		}
	}

	@Test
	public void testLoginWithCreds() throws URISyntaxException, ValidationError, SQLException {
		String actualUser = testLogin(userUrl(false), USER, PASSWORD);
		assertEquals(USER, actualUser);
	}

	@Test
	public void testLoginWithoutCreds() throws URISyntaxException, ValidationError, SQLException {
		String actualUser = testLogin(userUrl(true), null, null);
		assertEquals(USER, actualUser);
	}

	@Test
	public void testLoginAsAdmin() throws SQLException {
		String actualUser = testLogin(Config.databaseUrl(), null, null);
		// Most likely 'monetdb' but we can't be sure what the user configured
		assertNotEquals(USER, actualUser);
	}

	private void testGetColumnTypes(String table, ColumnDescr[] expected) throws URISyntaxException, ValidationError, SQLException {
		Destination dest = new Destination(userUrl(true), null, null, table);
		try (Connection conn = dest.connect()) {
			ColumnDescr[] colTypes = dest.getColumnTypes();
			assertArrayEquals(expected, colTypes);
		}
	}

	@Test
	public void testGetColumnTypes_qualified() throws SQLException, URISyntaxException, ValidationError {
		String qname = quoteId(SCHEMA) + "." + quoteId(TABLE);
		testGetColumnTypes(qname, QUALIFIED_TABLE_TYPE);
	}

	@Test
	public void testGetColumnTypes_implied() throws SQLException, URISyntaxException, ValidationError {
		String qname = quoteId(TABLE);
		testGetColumnTypes(qname, SYS_TABLE_TYPE);
	}
}