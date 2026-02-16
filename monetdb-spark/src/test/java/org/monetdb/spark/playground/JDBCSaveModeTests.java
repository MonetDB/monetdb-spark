package org.monetdb.spark.playground;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JDBCSaveModeTests extends AbstractSaveModeTests {

	@Override
	protected String getSource() {
		return "jdbc";
	}

	@Test
	public void testErrorIfExistsWhenPresent() throws SQLException {
		setUpTable(true);
		AnalysisException exc = assertThrows(AnalysisException.class, () -> insertData(SaveMode.ErrorIfExists));
		String msg = exc.getMessage();
		assertTrue(msg.matches(".*already exists.*"), msg);
		// INT because Spark shouldn't have touched the table
		verifyData("INT", true, false);
	}

	@Test
	public void testErrorIfExistsWhenNotPresent() throws SQLException {
		setUpTable(false);
		insertData(SaveMode.ErrorIfExists);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}

	@Test
	public void testIgnoreWhenPresent() throws SQLException {
		setUpTable(true);
		insertData(SaveMode.Ignore);
		// INT because Spark should leave the existing table alone
		verifyData("INT", true, false);
	}

	@Test
	public void testIgnoreWhenNotPresent() throws SQLException {
		setUpTable(false);
		insertData(SaveMode.Ignore);
		// BIGINT because Spark creates the table
		verifyData("BIGINT", false, true);
	}
}
