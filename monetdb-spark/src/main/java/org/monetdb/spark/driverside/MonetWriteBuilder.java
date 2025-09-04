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

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Helper class that extends {@link MonetTable} with information specific to writing
 * <p>
 * In the future we can implement some helper interfaces such as
 * {@link org.apache.spark.sql.connector.write.SupportsOverwrite} and
 * {@link org.apache.spark.sql.connector.write.SupportsTruncate}.
 * Right now it only implements {@link #build()} which builds a {@link Write}.
 */
class MonetWriteBuilder implements WriteBuilder {
	private final Parms parms;

	public MonetWriteBuilder(Parms parms) {
		this.parms = parms;
	}

	@Override
	public Write build() {
		// The user has done a lot of method chaining:
		//     dataframe
		//         .write
		//         .mode(...)
		//         .option(...)
		//         .option(...)
		//         .save().
		// We have now reached the save().
		return new MonetWrite(parms);
	}
}
