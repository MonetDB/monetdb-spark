package org.monetdb.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Spark Data Source for efficiently writing data to MonetDB
 */
public class DefaultSource implements TableProvider {
	@Override
	public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
		// Method supportsExternalMetadata() returns 'true' so this method won't be called.
		throw new RuntimeException("inferSchema() not implemented, go away!");
	}

	@Override
	public boolean supportsExternalMetadata() {
		// Return true so inferSchema() won't be called.
		// Instead we'll look at the dataframe schema and the table schema
		// and see if the dataframe can be converted to something the table
		// can hold.
		return true;
	}

	@Override
	public Table getTable(StructType structType, Transform[] partitioning, Map<String, String> map) {
		throw new RuntimeException("not implemented yet");
	}
}
