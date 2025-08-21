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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class MockRow implements SpecializedGetters {

	public final Object[] values;

	public MockRow(Object... values) {
		this.values = values;
	}

	@Override
	public boolean isNullAt(int i) {
		return values[i] == null;
	}

	@Override
	public boolean getBoolean(int i) {
		return (Boolean) values[i];
	}

	@Override
	public byte getByte(int i) {
		return (Byte) values[i];
	}

	@Override
	public short getShort(int i) {
		return (Short) values[i];
	}

	@Override
	public int getInt(int i) {
		return (Integer) values[i];
	}

	@Override
	public long getLong(int i) {
		return (Long) values[i];
	}

	@Override
	public float getFloat(int i) {
		return (Float) values[i];
	}

	@Override
	public double getDouble(int i) {
		return (Double) values[i];
	}

	@Override
	public Decimal getDecimal(int i, int i1, int i2) {
		throw new UnsupportedOperationException(",");
	}

	@Override
	public UTF8String getUTF8String(int i) {
		String s = (String) values[i];
		return UTF8String.fromString(s);

	}

	@Override
	public byte[] getBinary(int i) {
		throw new UnsupportedOperationException("getBinary");
	}

	@Override
	public CalendarInterval getInterval(int i) {
		throw new UnsupportedOperationException("getInterval");
	}

	@Override
	public VariantVal getVariant(int i) {
		throw new UnsupportedOperationException("getVariant");
	}

	@Override
	public InternalRow getStruct(int i, int i1) {
		throw new UnsupportedOperationException("getStruct");
	}

	@Override
	public ArrayData getArray(int i) {
		throw new UnsupportedOperationException("getArray");
	}

	@Override
	public MapData getMap(int i) {
		throw new UnsupportedOperationException("getMap");
	}

	@Override
	public Object get(int i, DataType dataType) {
		throw new UnsupportedOperationException("get");
	}
}
