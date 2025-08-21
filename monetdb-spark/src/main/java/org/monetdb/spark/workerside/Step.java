/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.workerside;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

import java.io.Serializable;

/**
 * One step in the extraction plan
 */
public interface Step extends Serializable {
	/**
	 * Invoked before the first execution of the plan.
	 *
	 * @param collector Allows the Step to take references to buffers inside the Collector
	 */
	default void init(Collector collector) {
	}

	/**
	 * Invoked for each row.
	 *
	 * @param row The current input row, in case this step needs it.
	 */
	void exec(SpecializedGetters row);
}
