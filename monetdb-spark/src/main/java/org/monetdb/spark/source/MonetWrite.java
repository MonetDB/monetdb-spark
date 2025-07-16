package org.monetdb.spark.source;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Everything we know about what to write where, and how.
 *
 * Constructed in the driver when save() is finally called when writing a dataframe.
 *
 * If we would support truncate and overwrite (creating a new table), we'd
 * do it here. For now, we just check if the dataframe type is suitable
 * to be converted to the column types of the existing table.
 *
 * The most important method is {@link toBatch}, which starts the actual
 * uploading of the data.
 */
public class MonetWrite implements Write {
	private final Map<String, String> props;
	private final StructType structType;
	private final Transform[] partitioning;
	private final LogicalWriteInfo logicalWriteInfo;

	public MonetWrite(Map<String, String> props, StructType structType, Transform[] partitioning, LogicalWriteInfo logicalWriteInfo) {
		this.props = props;
		this.structType = structType;
		this.partitioning = partitioning;
		this.logicalWriteInfo = logicalWriteInfo;
	}
}
