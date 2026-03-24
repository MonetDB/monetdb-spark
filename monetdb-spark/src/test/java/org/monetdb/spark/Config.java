package org.monetdb.spark;

import org.apache.spark.sql.SparkSession;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;


public class Config {
	public static final String PROPERTIES_FILE_NAME = "override.properties";

	private static final String DB_PROPERTY = "test.db";
	private static final String DB_DEFAULT = "testspark";

	private static final String SPARK_MASTER_PROPERTY = "test.spark";
	private static final String SPARK_MASTER_DEFAULT = "local[4]";

	private static final String SPARK_PARTITIONS_PROPERTY = "test.partitions";
	private static final String SPARK_PARTITIONS_DEFAULT = "-1";

	private static final String VERBOSE_PROPERTY = "test.verbose";
	private static final String VERBOSE_DEFAULT = "false";

	private static final String TORTURE_DURATION_PROPERTY = "test.torture.duration";
	private static final String TORTURE_DURATION_DEFAULT = "3.0";

	private static final Properties fileProperties = new Properties();
	private static boolean filePropertiesLoaded = false;

	private static String getProperty(String key, String defaultValue) {
		synchronized (fileProperties) {
			if (!filePropertiesLoaded) {
				try {
					fileProperties.load(new FileReader(PROPERTIES_FILE_NAME));
				} catch (IOException ignored) {
				}
				filePropertiesLoaded = true;
			}
		}
		return fileProperties.getProperty(key, System.getProperty(key, defaultValue));
	}

	public static String databaseUrl(String... queryParts) {
		String url = databaseUrl();
		String sep = url.contains("?") ? "&" : "?";
		StringBuilder urlBuilder = new StringBuilder(url);
		for (String part : queryParts) {
			if (part.isEmpty())
				continue;
			urlBuilder.append(sep).append(part);
			sep = "&";
		}
		return urlBuilder.toString();
	}

	public static String databaseUrl() {
		String db = getProperty(DB_PROPERTY, DB_DEFAULT);
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
		return getProperty(SPARK_MASTER_PROPERTY, SPARK_MASTER_DEFAULT);
	}

	public static SparkSession sparkSession() {
		String url = sparkUrl();
		return SparkSession.builder().appName("unit_tests").config("spark.master", url).getOrCreate();
	}

	/**
	 * Used to reduce the number of partitions when writing.
	 * Only some tests obey this
	 *
	 * @return maximum number of partitions to use, or -1 if not set.
	 */
	public static int sparkPartitions() {
		String num = getProperty(SPARK_PARTITIONS_PROPERTY, SPARK_PARTITIONS_DEFAULT);
		int n = Integer.parseInt(num);
		return n;
	}

	public static boolean verbose() {
		String boolval = getProperty(VERBOSE_PROPERTY, VERBOSE_DEFAULT);
		boolean b = Boolean.parseBoolean(boolval);
		return b;
	}

	public static int tortureDurationMillis() {
		String num = getProperty(TORTURE_DURATION_PROPERTY, TORTURE_DURATION_DEFAULT);
		double d = Double.parseDouble(num);
		if (d > 0)
			return (int)(1000 * d);
		else
			throw new IllegalArgumentException("Torture test duration '" + TORTURE_DURATION_PROPERTY + "' must be > 0.0");
	}


	public static boolean supportsCompression(Connection conn, String compression) throws SQLException {
		boolean origAutoCommit = conn.getAutoCommit();
		Savepoint savePoint = null;
		try {
			conn.setAutoCommit(false);
			savePoint = conn.setSavepoint();
			String create = "DROP TABLE IF EXISTS foo; CREATE TABLE checkifcompressionsupported(i INT)";
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(create);
			}
			String prepare = "COPY BINARY INTO checkifcompressionsupported FROM 'bla' ON '" + compression + "' CLIENT";
			try {
				conn.prepareStatement(prepare).close();
				return true;
			} catch (SQLException e) {
				return false;
			}
		} finally {
			if (savePoint != null) {
				conn.rollback(savePoint);
			}
			conn.setAutoCommit(origAutoCommit);
		}
	}
}
