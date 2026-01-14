package org.monetdb.spark.cli;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructField;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;

public class UploadTool {
	public static final String USAGE = "Usage: UploadTool OPTIONS\n" + "OPTIONS:\n" + "    -d JDBC_URL\n" + "    -s PATH_" + "    -t TABLENAME\n" + "    [" + "-w]                wait for keypress before and after uploading\n";
	private final String dburl;
	private final String tableName;
	private final String source;
	private final boolean waitKeyPress;

	private long startTime = 0;
	private SparkSession spark;
	private BufferedReader stdinReader;

	public UploadTool(String dburl, String tableName, String source, boolean waitKeyPress) {
		this.dburl = dburl;
		this.tableName = tableName;
		this.source = source;
		this.waitKeyPress = waitKeyPress;
	}

	public static void main(String[] args) throws SQLException, IOException {
		String dburl = null;
		String table = null;
		String source = null;
		boolean waitKeyPress = false;

		for (int i = 0; i < args.length; i++) {
			var arg = args[i];
			if (arg.equals("-d")) {
				dburl = args[++i];
			} else if (arg.equals("-s")) {
				source = args[++i];
			} else if (arg.equals("-t")) {
				table = args[++i];
			} else if (arg.equals("-w")) {
				waitKeyPress = true;
			} else {
				croak(new String[]{"Invalid option: " + arg, USAGE});
			}
		}
		if (dburl == null)
			croak("Database URL missing, specify with -d");
		if (table == null)
			croak("Target table missing, specify with -t");
		if (source == null)
			croak("Data source missing, specify with -s");
		var tool = new UploadTool(dburl, table, source, waitKeyPress);
		tool.run();
	}

	private static void croak(String msg) {
		croak(new String[]{msg});
	}

	private static void croak(String[] msgs) {
		System.err.print("Error: ");
		for (String msg : msgs) {
			System.err.println(msg.stripTrailing());
		}
		System.exit(1);
	}

	public static String quoteId(String id) {
		return "\"" + id.replace("\"", "\"\"") + "\"";
	}

	private double measureTime(String phase) {
		double duration = 0.0;
		long now = System.currentTimeMillis();
		if (startTime != 0 && phase != null) {
			duration = (now - startTime) / 1000.0;
			System.out.printf("* %s took %.03fs%n", phase, duration);
			System.out.println();
		}
		startTime = now;
		return duration;
	}

private void run() throws SQLException, IOException {
		measureTime(null);
		initSpark();
		measureTime("Initializing Spark");

		Dataset<Row> data = loadData();
		measureTime("Loading data");

		createTable(data);
		measureTime("Creating table");

		maybeAwaitKeypress();
		measureTime(null);
		uploadData(data);
		measureTime("Uploading data");
		maybeAwaitKeypress();
	}

	private void initSpark() {
		spark = SparkSession.builder().appName("UploadTool").config("spark.master", "local[4]").getOrCreate();
	}

	private Dataset<Row> loadData() {
		Dataset<Row> data = spark.read().format("orc").load(source);
		int colCount = data.schema().size();
		long rowCount = data.count();
		System.out.println("Data has " + colCount + " columns");
		System.out.println("Data has " + rowCount + " rows");
		return data;
	}

	private void createTable(Dataset<Row> data) throws SQLException {
		String qTable = quoteId(tableName);
		String fieldDefs = extractSchema(data);
		try (Connection conn = DriverManager.getConnection(dburl); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS " + qTable);
			stmt.execute("CREATE TABLE " + qTable + " (" + fieldDefs + ")");
		}
	}

	private String extractSchema(Dataset<Row> data) {
		JdbcDialect dialect = JdbcDialects.get(dburl);
		StructField[] schema = data.schema().fields();
		String qTable = quoteId(tableName);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < schema.length; i++) {
			StructField field = schema[i];
			String qColName = quoteId(field.name());
			String colType = JdbcUtils.getJdbcType(field.dataType(), dialect).databaseTypeDefinition();
			String comma = i < schema.length - 1 ? "," : "";
			sb.append("\t").append(qColName).append(" ").append(colType).append(comma).append("\n");
		}
		return sb.toString();
	}

	private void maybeAwaitKeypress() throws IOException {
		if (!waitKeyPress)
			return;
		System.out.println("Press a key to continue>");
		if (stdinReader == null)
			stdinReader = new BufferedReader(new InputStreamReader(System.in));
		stdinReader.readLine();
	}

	private void uploadData(Dataset<Row> data) throws SQLException {
		data.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", dburl)
				.option("dbtable", tableName)
				.option("batchsize", 10_000)
				.save();
		long dataRowCount = data.count();

		long tableRowCount;
		String qTable = quoteId(tableName);
		try (Connection conn = DriverManager.getConnection(dburl); Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + qTable)) {
				rs.next();
				tableRowCount = rs.getLong(1);
			}
		}
		System.out.println("Inserted " + dataRowCount + " rows, table " + qTable + " now has " + tableRowCount + " rows");
	}
}
