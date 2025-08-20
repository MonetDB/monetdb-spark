/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RangeTest {
	@Test
	public void testConstructor2() {
		Range r = new Range(-3, 6);
		assertEquals(-3, r.lo);
		assertEquals(6, r.hi);

		r = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		assertEquals(Long.MIN_VALUE, r.lo);
		assertEquals(Long.MAX_VALUE, r.hi);
	}

	@Test
	public void testConstructor1() {
		Range r = new Range(42);
		assertEquals(-42, r.lo);
		assertEquals(42, r.hi);
	}

	@Test
	void forDecimal() {
		Range r = Range.forDecimalWidth(3);
		assertEquals(-999, r.lo);
		assertEquals(999, r.hi);

		r = Range.forDecimalWidth(18);
		assertEquals(-999_999_999_999_999_999L, r.lo);
		assertEquals(999_999_999_999_999_999L, r.hi);
	}

	@Test
	void forDecimalOverFlow() {
		RuntimeException e = assertThrows(RuntimeException.class, () -> Range.forDecimalWidth(19));
		String msg = e.toString();
		assertTrue(msg.contains("overflow, width 19"), msg);
	}

	@Test
	public void testForIntegerSymmetric() {
		Range r = Range.forIntegerSymmetric(16);
		assertEquals(-32_767, r.lo);
		assertEquals(32_767, r.hi);
		r = Range.forIntegerSymmetric(64);
		assertEquals(-Long.MAX_VALUE, r.lo);
		assertEquals(Long.MAX_VALUE, r.hi);
	}

	@Test
	public void testForIntegerFull() {
		Range r = Range.forIntegerFull(16);
		assertEquals(-32_768, r.lo);
		assertEquals(32_767, r.hi);
		r = Range.forIntegerFull(64);
		assertEquals(Long.MIN_VALUE, r.lo);
		assertEquals(Long.MAX_VALUE, r.hi);
	}

	@Test
	public void testContains() {
		Range d9 = Range.forDecimalWidth(9);
		Range d10 = Range.forDecimalWidth(10);
		Range i32 = Range.forIntegerSymmetric(32);
		assertTrue(d9.contains(d9));
		assertTrue(d10.contains(d10));
		assertTrue(i32.contains(i32));
		assertTrue(d9.contains(d9));
		assertTrue(i32.contains(d9));
		assertFalse(i32.contains(d10));
	}
}