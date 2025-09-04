/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

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
	private final Parms parms;

	public MonetTable(Parms parms) {
		this.parms = parms;
	}

	@Override
	public String name() {
		return parms.getTableName();
	}

	@Override
	@Deprecated // it's deprecated but we must implement it
	public StructType schema() {
		return parms.getStructType();
	}

	@Override
	public Transform[] partitioning() {
		// Not sure what this is, just return what was given when we were created
		return parms.getPartitioning();
	}

	@Override
	public Map<String, String> properties() {
		return parms.getMap();
	}

	@Override
	public Set<TableCapability> capabilities() {
		return Set.of(TableCapability.BATCH_WRITE);
	}

	@Override // SupportsWrite
	public WriteBuilder newWriteBuilder(LogicalWriteInfo ignored) {
		// Caller has made up their mind that we're going to write.
		// Create a new object to celebrate.
		return new MonetWriteBuilder(parms);
	}
}
