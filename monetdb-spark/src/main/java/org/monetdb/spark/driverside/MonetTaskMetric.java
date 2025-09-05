/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.driverside;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class MonetTaskMetric implements CustomTaskMetric {
	private final String name;
	private final long value;

	public MonetTaskMetric(String name, long value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public long value() {
		return value;
	}
}
