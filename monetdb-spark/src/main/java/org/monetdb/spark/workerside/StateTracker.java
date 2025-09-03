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

public class StateTracker {
	private final long[] millis;
	private long nrows;
	private long nuploads;

	private State currentState = null;
	private long currentStateStarted = 0;

	private StateTracker(long[] counters) {
		millis = counters.clone();
	}

	public StateTracker() {
		this(new long[State.values().length]);
	}

	public State getState() {
		return currentState;
	}

	public State setState(State state) {
		return setState(state, System.currentTimeMillis());
	}

	public State setState(State newState, long now) {
		State prevState = currentState;
		if (prevState != null)
			millis[prevState.ordinal()] += now - currentStateStarted;
		currentState = newState;
		currentStateStarted = now;
		return prevState;
	}

	public void addRow() {
		nrows += 1;
	}

	public void addUpload() {
		nuploads += 1;
	}

	public long duration(State state) {
		return millis[state.ordinal()];
	}

	public long rowCount() {
		return nrows;
	}

	public long uploadCount() {
		return nuploads;
	}

	@Override
	public String toString() {
		long total = 0;
		long collecting = duration(State.Collecting);
		total += collecting;
		long uploading = duration(State.Uploading);
		total += uploading;
		long server = duration(State.Server);
		total += server;
		long committing = duration(State.Committing);
		total += committing;
		return "(uploads=%d rows=%d millisCollecting=%d millisUploading=%d millisServer=%d millisCommitting=%d millisTotal=%d)".formatted(
				nuploads, nrows, collecting, uploading, server, committing, total);
	}
}
