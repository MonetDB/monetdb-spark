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

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.monetdb.spark.common.CompressionSettings;
import org.monetdb.spark.common.Destination;

import java.io.Serializable;
import java.util.Map;

public class Parms implements Serializable {
	private final Map<String, String> map;
	private final StructType structType;
	private final Transform[] partitioning;
	private final String tableName;
	private final String url;
	private final String user;
	private final String password;
	private final long batchSize;
	private final boolean immediateCommit;
	private final boolean allowOverflow;
	private final String dumpdir;
	private final String dumpprefix;
	private final boolean dumponserver;
	private final CompressionSettings compressionSettings;

	public Parms(StructType structType, Transform[] partitioning, Map<String, String> parameterMap) {
		this.map = parameterMap;
		this.structType = structType;
		this.partitioning = partitioning;
		tableName = argument("dbtable");

		String batchSizeArg = argument("batchsize", null);
		batchSize = batchSizeArg != null ? Long.parseLong(batchSizeArg) : Long.MAX_VALUE;

		String allowOverflowArg = argument("allowoverflow", "true");
		allowOverflow = Boolean.parseBoolean(allowOverflowArg);

		String immediateCommitArg = argument("immediatecommit", "false");
		immediateCommit = Boolean.parseBoolean(immediateCommitArg);

		url = argument("url");
		user = argument("user", null);
		password = argument("password", null);

		dumpdir = argument("dumpdir", null);
		dumpprefix = argument("dumpprefix", dumpdir);
		String dumponserverArg = argument("dumponserver", "false");
		dumponserver = Boolean.parseBoolean(dumponserverArg);

		String compression = argument("compression", "none");
		compressionSettings = new CompressionSettings(compression);

		if (dumpdir != null && immediateCommit)
			throw new RuntimeException("dumpdir cannot be combined with immediatecommit");
		if (dumpprefix != null && dumpdir == null)
			throw new RuntimeException("dumpprefix can only be used with dumpdir");
		if (dumponserver && dumpdir == null)
			throw new RuntimeException("dumponserver can only be used with dumpdir");
	}

	public Destination getDestination() {
		return new Destination(getUrl(), getUser(), getPassword(), getTableName());
	}

	private String argument(String key) {
		String value = map.get(key);
		if (value == null)
			throw new IllegalArgumentException("Option '" + key + "' is required");
		return value;
	}

	private String argument(String key, String defaultValue) {
		Object x = getMap().getOrDefault(key, defaultValue);
		return x == null ? null : x.toString();
	}


	public Map<String, String> getMap() {
		return map;
	}

	public StructType getStructType() {
		return structType;
	}

	public Transform[] getPartitioning() {
		return partitioning;
	}

	public String getTableName() {
		return tableName;
	}

	public String getUrl() {
		return url;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public boolean isAllowOverflow() {
		return allowOverflow;
	}

	public boolean isImmediateCommit() {
		return immediateCommit;
	}

	public String getDumpdir() {
		return dumpdir;
	}

	public String getDumpPrefix() {
		return dumpprefix;
	}

	public boolean isDumpOnServer() {
		return dumponserver;
	}

	public CompressionSettings getCompressionSettings() {
		return compressionSettings;
	}
}
