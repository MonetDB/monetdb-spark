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
	public void testOverwriteWhenPresent() throws SQLException {
		super.testOverwriteWhenPresent();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testOverwriteWhenNotPresent() throws SQLException {
		super.testOverwriteWhenNotPresent();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testForeignKeyWithoutTruncate() throws SQLException {
		super.testForeignKeyWithoutTruncate();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testForeignKeyWithTruncateCascade() throws SQLException {
		super.testForeignKeyWithTruncateCascade();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testForeignKeyWithTruncateNoData() throws SQLException {
		super.testForeignKeyWithTruncateNoData();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testForeignKeyWithTruncateWithData() throws SQLException {
		super.testForeignKeyWithTruncateWithData();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testOverwriteTruncateWhenNotPresent() throws SQLException {
		super.testOverwriteTruncateWhenNotPresent();
	}

	@Test
	@Disabled("mode doesn't work yet")
	@Override
	public void testOverwriteTruncateWhenPresent() throws SQLException {
		super.testOverwriteTruncateWhenPresent();
	}
}
