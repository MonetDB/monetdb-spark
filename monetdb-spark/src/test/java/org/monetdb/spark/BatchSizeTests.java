/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark;

import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Verify batchsize and immediatecommit by enabling the jdbc log and
 * examining the log files.
 */
public class BatchSizeTests {
	private final Pattern starsPattern = Pattern.compile("[*]{2,}", Pattern.DOTALL);

	@TempDir
	Path scratchDir;
	private long maxStars;
	private long commitCount;
	private boolean skip;

	public static boolean atLeastJdbc_12_1() throws SQLException {
		// monetdb-jdbc 12.1 fixes a bug in the logging configuration.
		Driver driver = DriverManager.getDriver(Config.databaseUrl());
		int major = driver.getMajorVersion();
		int minor = driver.getMinorVersion();
		return major > 12 || (major == 12 && minor >= 1);
	}

	@Test
	@EnabledIf(value = "atLeastJdbc_12_1", disabledReason = "needs JDBC 12.1 or newer")
	public void testNoBatchSize() throws IOException, SQLException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.save();
		readLogs();
		assertEquals(1, commitCount);
		// we expect at least one block full of stars
		assertEquals(8190, maxStars);
	}

	@Test
	@EnabledIf(value = "atLeastJdbc_12_1", disabledReason = "needs JDBC 12.1 or newer")
	public void testWithBatchSize() throws SQLException, IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("batchsize", "1000").save();
		readLogs();
		assertEquals(1, commitCount);
		// batch size limits it to 1000
		assertEquals(1000, maxStars);
	}

	@Test
	@EnabledIf(value = "atLeastJdbc_12_1", disabledReason = "needs JDBC 12.1 or newer")
	public void testImmediateCommit() throws SQLException, IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("batchsize", "1000").option("immediatecommit", "true").save();
		readLogs();
		// commit count should be at least 10
		assertNotEquals(0, commitCount);
		assertNotEquals(1, commitCount);
		// batch size limits it to 1000
		assertEquals(1000, maxStars);
	}


	private DataFrameWriter<Row> makeWriter() throws SQLException {
		try (Connection conn = Config.connectDatabase(); Statement stmt = conn.createStatement()) {
			stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo (x TINYINT)");
		}

		Path logfile = scratchDir.resolve("logfile.txt");
		String escapedLogfile = URLEncoder.encode(logfile.toString(), StandardCharsets.UTF_8);
		String url = Config.databaseUrl("logfile=" + escapedLogfile, "debug=true");
		SparkSession spark = Config.sparkSession();
		Dataset<Row> df = spark.range(20_000).withColumn("x", lit(42).cast("BYTE")).drop("id").repartition(1);
		DataFrameWriter<Row> writer = df
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", url)
				.option("dbtable", "foo");
		return writer;
	}

	private void readLogs() throws IOException {
		try (DirectoryStream<Path> entries = Files.newDirectoryStream(scratchDir, "logfile*.txt")) {
			for (Path entry : entries) {
				readLog(entry);
			}
		}

	}

	private void readLog(Path logfile) throws IOException {
		skip = false;
		try (Stream<String> lines = Files.lines(logfile)) {
			lines.forEach(this::readLine);

		}
	}

	private void readLine(String line) {
		if (skip)
			return;
		if (line.contains("-- get column types"))
			skip = true;
		if (line.contains("sCOMMIT"))
			commitCount++;
		Matcher matcher = starsPattern.matcher(line);
		while (matcher.find()) {
			int len = matcher.group(0).length();
			if (len > maxStars)
				maxStars = len;
		}
	}

}
