/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright MonetDB Solutions B.V.
 */

/*
 * Copyright (c) MonetDB Solutions
 */

package org.monetdb.spark.bincopy;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.monetdb.spark.driverside.Parms;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.MonetDataWriter;
import org.monetdb.spark.workerside.Step;

import java.io.Serial;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * Passed from driver to executors, holds everything the executors must know.
 */
public class BinCopyDataWriterFactory implements DataWriterFactory, Serializable {
	@Serial
	private static final long serialVersionUID = 0L;

	private final Parms parms;
	private final String[] columns;
	private final Step[] steps;

	public BinCopyDataWriterFactory(Parms parms, String[] columns, Step[] steps) {
		this.parms = parms;
		this.columns = columns;
		this.steps = steps;
	}

	@Override
	public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
		try {
			String identifier = "part" + partitionId + "-task" + taskId;
			Collector collector = new Collector();
			BinCopyUploader uploader = new BinCopyUploader(parms.getDestination(), collector, identifier, columns);
			collector.registerWithConverters(steps);
			return new MonetDataWriter(collector, steps, uploader, parms.isImmediateCommit(), identifier, parms.getBatchSize());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
