/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.monetdb.spark.bincopy.BinCopySql;
import org.monetdb.spark.bincopy.Plan;
import org.monetdb.spark.common.ColumnDescr;
import org.monetdb.spark.common.CompressionSettings;
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.util.WithSavepoint;
import org.monetdb.spark.workerside.ConversionError;
import org.monetdb.spark.workerside.StateTrackerMetric;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Everything we know about what to write where, and how.
 * <p>
 * Constructed in the driver when save() is finally called when writing a dataframe.
 * <p>
 * If we want to support more than one way of uploading, for example COPY INTO
 * in addition to COPY BINARY INTO, this is where we'd decide what to use.
 * <p>
 * The most important method is toBatch, which starts the actual
 * uploading of the data.
 */
public class MonetWrite implements Write {
	private final Parms parms;
	private final Plan plan;
	private final BinCopySql sqlstmt;

	public MonetWrite(Parms parms, boolean dropExisting) {
		this.parms = parms;
		Destination dest = parms.getDestination();

		// Create a connection to check the existing table and possibly modify it.
		// Ensure everything is rolled back if something goes wrong.
		try (Connection conn = dest.connect(); WithSavepoint sp = new WithSavepoint(conn)) {
			// Create or replace the table if necessary, and get the JDBC types of the columns
			ColumnDescr[] columnDescrs = findOrCreateTable(conn, parms, dropExisting);

			// Build the conversion plan based on the schema we have and the column types
			// in the database
            plan = new Plan(parms.getStructType().fields(), columnDescrs, parms.isAllowOverflow());

			// Construct the COPY statement we will use, and test if the server accepts it
			sqlstmt = new BinCopySql(dest.getTable(), plan.getColumns());
            // Test it without compression
			PreparedStatement ps1 = conn.prepareStatement("-- validate COPY statement\n" + sqlstmt);
			ps1.close();

            CompressionSettings compression = parms.getCompressionSettings();
            if (compression.algo() != null) {
                sqlstmt.compression(compression);
                // Test it with compression enabledl
                try {
					PreparedStatement ps2 = conn.prepareStatement("-- does server support COPY statement with compression?\n" + sqlstmt);
					ps2.close();
                } catch (SQLException e) {
                    throw new RuntimeException("Server does not support compression algorithm '" + compression.algo() + "'");
                }
            }

			// Everything went ok, allow any changes to be committed.
			sp.setCommitOnClose(true);
        } catch (SQLException | ConversionError e) {
			// Spark doesn't allow us to throw checked exceptions
			throw new RuntimeException(e);
		}
	}

	private static ColumnDescr[] findOrCreateTable(Connection conn, Parms parms, boolean dropExisting) throws SQLException {
		Destination dest = parms.getDestination();

		// Get the existing column types within a savepoint since it's allowed to fail.
		ColumnDescr[] existingColumns = null;
		try (WithSavepoint sp = new WithSavepoint(conn)) {
			existingColumns = parms.getDestination().getColumns(conn);
		} catch (SQLException e) {
			// It's ok if the table doesn't exist but all other errors must be rethrow
			if (!e.getSQLState().equals("42S02")) {
				throw e;
			}
		}

		// We have to choose between:
		// 1) CREATE because the table does not exist
		// 2) do nothing because it exists and dropExisting is false
		// 3) TRUNCATE RESTRICT/CASCADE because it exists, dropExisting is true and we want to truncate
		// 4) DROP and CREATE because it exists, dropExisting is true and we don't want to truncate

		if (existingColumns == null) {
			// 1) CREATE because the table does not exist
			return planCreateTable(conn, parms);
		}
		if (!dropExisting) {
			// 2) do nothing because it exists and dropExisting is false
			return existingColumns;
		}
		if (parms.isTruncate()) {
			// 3) TRUNCATE RESTRICT/CASCADE because it exists, dropExisting is true and we want to truncate
			dest.truncateTable(conn, parms.isCascadeTruncate());
			return existingColumns;
		} else {
			// 4) DROP and CREATE because it exists, dropExisting is true and we don't want to truncate
			dest.dropTable(conn);
			return planCreateTable(conn, parms);
		}
	}

	private static ColumnDescr[] planCreateTable(Connection conn, Parms parms) throws SQLException {
		Destination dest = parms.getDestination();
		dest.createTable(conn, parms.getStructType());
		return dest.getColumns(conn);
	}

	@Override
	public BatchWrite toBatch() {
		return new MonetBatchWrite(parms, plan.getSteps(), sqlstmt);
	}

	@Override
	public CustomMetric[] supportedCustomMetrics() {
		CustomMetric[] superMetrics = Write.super.supportedCustomMetrics();
		CustomMetric[] customMetrics = StateTrackerMetric.METRICS;
		CustomMetric[] allMetrics = Stream
				.concat(Arrays.stream(superMetrics), Arrays.stream(customMetrics))
				.toArray(CustomMetric[]::new);
		return allMetrics;
	}
}
