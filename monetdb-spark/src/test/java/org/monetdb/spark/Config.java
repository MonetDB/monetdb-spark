package org.monetdb.spark;

public class Config {

	private static final String DEFAULT_DB = "testspark";

	public static String dbUrl() {
		String db = System.getProperty("test.db", DEFAULT_DB);
		if (!db.contains("/") && !db.contains(":"))
			db = "jdbc:monetdb://localhost/" + db;
		if (!db.contains("user=") && !db.contains("password=")) {
			db += db.contains("?") ? "&" : "?";
			db += "user=monetdb&password=monetdb";
		}
		return db;
	}
}
