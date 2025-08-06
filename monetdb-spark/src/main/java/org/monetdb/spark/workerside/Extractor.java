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
import org.monetdb.spark.bincopy.Collector;

import java.io.IOException;
import java.io.Serializable;

public interface Extractor extends Serializable {
	void init(Collector collector, int idx);

	void extract(SpecializedGetters row, int idx) throws IOException;
}
