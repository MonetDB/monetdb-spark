/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy.appenders;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

import java.time.LocalDate;

public class DateAppender extends Appender {
	public DateAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) {
		byte[] scratch = collector.scratchBuffer;

		// Any invalid pattern is NULL but the documentation mentions
		// MonetDB itself uses all-ones bits, so why not follow this?
		scratch[0] = -1; // day
		scratch[1] = -1; // month
		scratch[2] = -1; // year lo
		scratch[3] = -1; // year hi

		if (!collector.scratchNull) {
			LocalDate date = LocalDate.ofEpochDay((int) collector.scratchLong);
			scratch[0] = (byte) date.getDayOfMonth();
			scratch[1] = (byte) date.getMonthValue();
			int year = date.getYear();
			scratch[2] = (byte) year;
			scratch[3] = (byte) (year >> 8);
		}
		buffer.write(scratch, 0, 4);
	}
}
