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
import org.monetdb.spark.common.Destination;
import org.monetdb.spark.workerside.Collector;
import org.monetdb.spark.workerside.Converter;
import org.monetdb.spark.workerside.MonetDataWriter;

import java.io.Serial;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * Passed from driver to executors, holds everything the executors must know.
 */
public class BinCopyDataWriterFactory implements DataWriterFactory, Serializable {
	@Serial
	private static final long serialVersionUID = 0L;

	private final Destination dest;
	private final Converter[] converters;

	public BinCopyDataWriterFactory(Destination dest, Converter[] converters) {
		this.dest = dest;
		this.converters = converters;
	}

	@Override
	public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
		try {
			Collector collector = new Collector();
			collector.registerWithConverters(converters);
			BinCopyUploader uploader = new BinCopyUploader(dest, collector, converters);
			return new MonetDataWriter(collector, converters, uploader);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
