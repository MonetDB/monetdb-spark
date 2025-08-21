/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.common;

import org.monetdb.jdbc.MonetConnection;

import java.io.Serial;
import java.io.Serializable;
import java.sql.*;

public class Destination implements Serializable {
	@Serial
	private static final long serialVersionUID = 0L;

	private String url;
	private String user;
	private String password;
	private String table;

	public Destination(String url, String user, String password, String table) {
		this.url = url;
		this.user = user;
		this.password = password;
		this.table = table;
	}

	public MonetConnection connect() throws SQLException {
		Connection conn;
		if (user != null)
			conn = DriverManager.getConnection(url, user, password);
		else
			conn = DriverManager.getConnection(url);
		return conn.unwrap(MonetConnection.class);
	}

	public ColumnDescr[] getColumns() throws SQLException {
		// Connection databasemetadata is too tricky, it allows patterns,
		// works across schemas, how do we have to quote, etc.?
		// Instead, we just look at the types of SELECT * FROM table.
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			String sql = "SELECT * FROM " + table + " WHERE FALSE";
			try (ResultSet rs = stmt.executeQuery(sql)) {
				ResultSetMetaData md = rs.getMetaData();
				int n = md.getColumnCount();
				ColumnDescr[] ret = new ColumnDescr[n];
				for (int i = 1; i <= n; i++) {
					String name = md.getColumnName(i);
					JDBCType jdbcType = JDBCType.valueOf(md.getColumnType(i));
					int precision = md.getPrecision(i);
					int scale = md.getScale(i);
					String typeName = md.getColumnTypeName(i);
					ret[i - 1] = new ColumnDescr(name, jdbcType, precision, scale, typeName);
				}
				return ret;
			}
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
}
