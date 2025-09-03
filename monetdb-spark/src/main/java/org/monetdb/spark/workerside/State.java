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

/**
 * Enumerations of states the DataWriter can be in
 */
public enum State {
	/**
	 * Before the first row is offered to MonetDataWriter
	 */
	Initializing,
	/**
	 * While rows are being offered to MonetDataWriter
	 */
	Collecting,
	/**
	 * In the Uploader, when transferring data for the server
	 */
	Uploading,
	/**
	 * In the Uploader, while waiting for the server to ask for data
	 */
	Server,
	/**
	 * In the Uploader, while committing the transaction
	 */
	Committing,
}
