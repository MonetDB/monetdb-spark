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

public class Int8Appender extends IntegerLikeAppender {

	public Int8Appender(int index) {
		super(index);
	}

	@Override
	protected int sizeInBytes() {
		return 1;
	}

}
