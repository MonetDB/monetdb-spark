/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy;

import org.apache.spark.sql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.monetdb.jdbc.MonetConnection;
import org.monetdb.spark.Config;
import org.monetdb.spark.util.MyAutoClose;
import org.monetdb.util.FileTransferHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.*;

class BinCopyFileDumpTest {
	private static Path destDir;
	private final int NROWS = 100;
	@MyAutoClose
	MonetConnection conn;
	@MyAutoClose
	Statement stmt;

	@BeforeEach
	public void setUp(@TempDir Path tempDir) throws SQLException {
		destDir = tempDir;
		conn = Config.connectDatabase().unwrap(MonetConnection.class);
		stmt = conn.createStatement();
		stmt.execute("DROP TABLE IF EXISTS foo; CREATE TABLE foo(i INT, j INT)");
	}

	private Dataset<Row> makeDataframe() {
		SparkSession spark = Config.sparkSession();
		Dataset<Row> df = spark
				.range(NROWS)
				.withColumn("i", col("id").cast("INT"))
				.withColumn("j", col("i").multiply(lit(-1)))
				.drop("id");
		return df;
	}

	private DataFrameWriter<Row> makeWriter() {
		Dataset<Row> df = makeDataframe().repartition(3);
		DataFrameWriter<Row> writer = df
				.write()
				.format("org.monetdb.spark")
				.mode(SaveMode.Append)
				.option("url", Config.databaseUrl())
				.option("dbtable", "foo");
		return writer;
	}

	private Stream<Path> glob(String pattern) throws IOException {
		PathMatcher matcher = FileSystems.getDefault().getPathMatcher(pattern);
		return Files.walk(destDir).filter(matcher::matches);
	}

	private void uploadAndCheck() throws SQLException, IOException {
		FileTransferHandler handler = new FileTransferHandler(destDir, StandardCharsets.UTF_8);
		conn.setUploadHandler(handler);
		stmt.execute("TRUNCATE foo");

		for (Iterator<Path> it = glob("glob:**/copy.sql").iterator(); it.hasNext(); ) {
			String script = Files.readString(it.next());
			stmt.execute(script);
		}

		String sql = """
                SELECT
                	COUNT(i) AS nrows,
                	COUNT(DISTINCT i) AS nunique,
                	MIN(i) AS mini,
                	MAX(i) AS maxi
				FROM foo""";
		try (ResultSet rs = stmt.executeQuery(sql)) {
			assertTrue(rs.next());
			int nrows = rs.getInt("nrows");
			int nunique = rs.getInt("nunique");
			int mini = rs.getInt("mini");
			int maxi = rs.getInt("maxi");
			boolean ok = nrows == NROWS && nunique == NROWS && mini == 0 && maxi == NROWS -1;
			String msg = "nrows is %d (expected %d), nunique is %d (%d), min is %d (%d), max is %d (%d)".formatted(
					nrows, NROWS,
					nunique, NROWS,
					mini, 0,
					maxi, NROWS - 1);
			assertTrue(ok, msg);
			assertEquals(NROWS, nrows);
		}
	}

	private void checkOnClause(String onwhat) throws IOException {
		for (Iterator<Path> it = glob("glob:**/copy.sql").iterator(); it.hasNext(); ) {
			Path p = it.next();
			String onClause = null;
			for (String line : Files.readAllLines(p)) {
				if (line.startsWith("ON")) {
					onClause = line;
					break;
				}
			}
			assertNotNull(onClause);
			assertTrue(onClause.contains(onwhat), "looking for '" + onwhat + "', found: " + onClause);


		}
	}

	@Test
	public void testDump() throws SQLException, IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("dumpdir", destDir.toString()).save();

		uploadAndCheck();
	}

	@Test
	public void testDumpWithPrefix() throws SQLException, IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("dumpdir", destDir.toString()).option("dumpprefix", "").save();

		uploadAndCheck();
	}

	@Test
	public void testDumpOnDefault() throws IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("dumpdir", destDir.toString()).option("dumpprefix", "").save();

		checkOnClause("CLIENT");
	}

	@Test
	public void testDumpOnClient() throws IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("dumpdir", destDir.toString()).option("dumpprefix", "").option("dumponserver", "false").save();

		checkOnClause("CLIENT");
	}


	@Test
	public void testDumpOnServer() throws IOException {
		DataFrameWriter<Row> writer = makeWriter();
		writer.option("dumpdir", destDir.toString()).option("dumpprefix", "").option("dumponserver", "true").save();

		checkOnClause("SERVER");
	}
}