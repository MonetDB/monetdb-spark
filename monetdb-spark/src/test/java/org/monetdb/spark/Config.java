package org.monetdb.spark;

import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Config {

	private static final String DEFAULT_DB = "testspark";

	private static final String DEFAULT_SPARK = "local[4]";

	public static String databaseUrl() {
		String db = System.getProperty("test.db", DEFAULT_DB);
		if (!db.contains("/") && !db.contains(":"))
			db = "jdbc:monetdb://localhost/" + db;
		if (!db.contains("user=") && !db.contains("password=")) {
			db += db.contains("?") ? "&" : "?";
			db += "user=monetdb&password=monetdb";
		}
		return db;
	}

	public static Connection connectDatabase() throws SQLException {
		String url = databaseUrl();
		return DriverManager.getConnection(url);
	}

	public static void checkDatabase() throws SQLException {
		Connection conn = connectDatabase();
		conn.close();
	}

	public static String sparkUrl() {
		return System.getProperty("test.spark", DEFAULT_SPARK);
	}

	public static SparkSession sparkSession() {
		String url = sparkUrl();
		return SparkSession.builder()
				.appName("unit tests")
				.config("spark.master", url)
				.getOrCreate();
	}
}
