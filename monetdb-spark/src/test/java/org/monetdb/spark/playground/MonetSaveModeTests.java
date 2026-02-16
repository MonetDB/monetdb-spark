package org.monetdb.spark.playground;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class MonetSaveModeTests extends AbstractSaveModeTests {

	@Override
	protected String getSource() {
		return "org.monetdb.spark";
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testAppendWhenNotPresent() throws SQLException {
		super.testAppendWhenNotPresent();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testOverwriteWhenPresent() throws SQLException {
		super.testOverwriteWhenPresent();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testOverwriteWhenNotPresent() throws SQLException {
		super.testOverwriteWhenNotPresent();
	}
}
