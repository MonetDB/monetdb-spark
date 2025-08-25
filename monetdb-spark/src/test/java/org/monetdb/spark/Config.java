package org.monetdb.spark;

import org.apache.spark.sql.SparkSession;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

	private static final Properties fileProperties = new Properties();
	private static boolean filePropertiesLoaded = false;

	private static String getProperty(String key, String defaultValue) {
		synchronized (fileProperties) {
			if (!filePropertiesLoaded) {
				try {
					fileProperties.load(new FileReader(PROPERTIES_FILE_NAME));
					filePropertiesLoaded = true;
				} catch (IOException ignored) {
				}
			}
		}
		return fileProperties.getProperty(key, System.getProperty(key, defaultValue));
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
}
