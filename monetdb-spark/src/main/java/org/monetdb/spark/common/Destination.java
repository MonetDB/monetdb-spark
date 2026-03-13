/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.common;

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.MonetDialect;

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
        try (Connection conn = connect()) {
			return getColumns(conn);
		}
	}

	public ColumnDescr[] getColumns(Connection conn) throws SQLException {
		// Connection databasemetadata is too tricky, it allows patterns,
		// works across schemas, how do we quote, etc.?
		// Instead, we just look at the types of SELECT * FROM table.
		try (Statement stmt = conn.createStatement()) {
			String sql = "SELECT * FROM " + table + " WHERE FALSE -- get column types";
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

	public void truncateTable(Connection conn, boolean cascade) throws SQLException {
		String sql = "TRUNCATE " + table + " ";
		sql += cascade ? "CASCADE" : "RESTRICT";
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		}
	}

	public void dropTable(Connection conn) throws SQLException {
		String sql = "DROP TABLE " + table;
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		}
	}

	public void createTable(StructType schema) throws SQLException {
		try (Connection conn = connect()) {
			createTable(conn, schema);
		}
	}

	public void createTable(Connection conn, StructType schema) throws SQLException {
		String sql = tableDefinition(schema);
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		}
	}

	public String tableDefinition(StructType schema) {
		JdbcDialect dialect = new MonetDialect();
		StructField[] fields = schema.fields();
		String comma = "";
		StringBuilder sb = new StringBuilder("CREATE TABLE " + getTable() + "(");
		for (StructField field: fields) {
			sb.append("\n    ");
			sb.append(comma);
			comma = ",";
			sb.append(dialect.quoteIdentifier(field.name()));
			sb.append(" ");
			sb.append(JdbcUtils.getJdbcType(field.dataType(), dialect).databaseTypeDefinition());
		}
		sb.append("\n)");
		String sql = sb.toString();
		return sql;
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
