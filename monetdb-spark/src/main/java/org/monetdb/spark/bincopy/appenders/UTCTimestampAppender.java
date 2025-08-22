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

import java.time.*;

public class UTCTimestampAppender extends Appender {
	public UTCTimestampAppender(int index) {
		super(index);
	}

	@Override
	public void exec(SpecializedGetters row) {
		byte[] scratch = collector.scratchBuffer;


		if (collector.scratchNull) {
			// Any invalid pattern is NULL but the documentation mentions
			// MonetDB itself uses all-ones bits, so why not follow this?
			for (int i = 0; i < 12; i++)
				scratch[i] = -1;
		} else {
			// These are both always in UTC
			long lval = collector.scratchLong;
//			Instant instant = Instant.ofEpochMilli(lval);
			Instant instant = Instant.ofEpochSecond(0, 1000 * lval);

			// Now we have to be careful to render it in UTC
			OffsetDateTime odt = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
			// This is how the wire format is laid out:
			int microseconds = odt.getNano() / 1000;
			byte seconds = (byte) odt.getSecond();
			byte minutes = (byte) odt.getMinute();
			byte hours = (byte) odt.getHour();
			byte padding = 0;
			byte day = (byte) odt.getDayOfMonth();
			byte month = (byte) odt.getMonthValue();
			short year = (short) odt.getYear();
			// Now stuff it in the scratch buffer
			int i = 0;
			scratch[i++] = (byte) microseconds;
			scratch[i++] = (byte) (microseconds >> 8);
			scratch[i++] = (byte) (microseconds >> 16);
			scratch[i++] = 0; // 999_999_999 fits the lower 24 bits
			scratch[i++] = seconds;
			scratch[i++] = minutes;
			scratch[i++] = hours;
			scratch[i++] = padding;
			//
			scratch[i++] = day;
			scratch[i++] = month;
			scratch[i++] = (byte) year;
			scratch[i++] = (byte) (year >> 8);
		}
		buffer.write(scratch, 0, 12);
	}
}
