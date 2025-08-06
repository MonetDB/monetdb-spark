/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.monetdb.spark.common.Destination;

import java.util.Map;
import java.util.Set;

/**
 * This class keeps track of what we know of the Monet table the user wants to read or write.
 * <p>
 * It returns capability BATCH_WRITE implements {@link SupportsWrite} to
 * indicate that it can write. It can't read.
 * <p>
 * Only used in the driver.
 * <p>
 * The constructor just takes everything that was originally passed to
 * {@link org.monetdb.spark.DefaultSource}.
 */
public class MonetTable implements Table, SupportsWrite {
	private final String tableName;
	private final Map<String, String> props;
	private final StructType structType;
	private final Transform[] partitioning;

	public MonetTable(StructType structType, Transform[] partitioning, Map<String, String> props) {
		this.props = props;
		this.structType = structType;
		this.partitioning = partitioning;
		this.tableName = getArg("dbtable");
	}

	@Override
	public String name() {
		return tableName;
	}

	@Override
	@Deprecated // it's deprecated but we must implement it
	public StructType schema() {
		return structType;
	}

	@Override
	public Column[] columns() {
		// {@link SupportsWrite#columns} is really just {@link Table#columns},
		// which derives it from the structType.  Not sure if that's the right
		// thing to do for us.
		return SupportsWrite.super.columns();
	}

	@Override
	public Transform[] partitioning() {
		// Not sure what this is, just return what was given when we were created
		return partitioning;
	}

	@Override
	public Map<String, String> properties() {
		return props;
	}

	@Override
	public Set<TableCapability> capabilities() {
		return Set.of(TableCapability.BATCH_WRITE);
	}

	String getArg(String key) {
		String value = props.get(key);
		if (value == null)
			throw new IllegalArgumentException("Option '" + key + "' is required");
		return value;
	}

	String getArg(String key, String defaultValue) {
		return props.getOrDefault(key, defaultValue);
	}

	@Override // SupportsWrite
	public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
		// Caller has made up their mind that we're going to write.
		// Create a new object to celebrate.
		return new MonetWriteBuilder(logicalWriteInfo);
	}

	/**
	 * Helper class that extends {@link MonetTable} with information specific to writing
	 * <p>
	 * In the future we can implement some helper interfaces such as
	 * {@link org.apache.spark.sql.connector.write.SupportsOverwrite} and
	 * {@link org.apache.spark.sql.connector.write.SupportsTruncate}.
	 * Right now it only implements {@link #build()} which builds a {@link Write}.
	 */
	private class MonetWriteBuilder implements WriteBuilder {
		public MonetWriteBuilder(LogicalWriteInfo ignored) {
		}

		@Override
		public Write build() {
			// The user has done a lot of method chaining:
			//     dataframe
			//         .write
			//         .mode(...)
			//         .option(...)
			//         .option(...)
			//         .save().
			// We have now reached the save().
			String url = getArg("url");
			String user = getArg("user", null);
			String password = getArg("password", null);
			Destination dest = new Destination(url, user, password, tableName);

			return new MonetWrite(dest, structType);
		}
	}
}
