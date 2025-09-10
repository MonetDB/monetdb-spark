/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

package org.monetdb.spark.bincopy;

import java.io.IOException;
import java.sql.SQLException;

public interface Uploader {
	void uploadBatch() throws SQLException, IOException;

	void commit() throws SQLException, IOException;

	void close() throws SQLException, IOException;

	void setOnStartUpload(Runnable callback);

	void setOnEndUpload(Runnable callback);
}
