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
	private final String identifier;
	private final long[] millis;
	private long total;
	private long nrows;
	private long nuploads;

	private State currentState = null;
	private long currentStateStarted = 0;

	private StateTracker(String identifier, long[] counters) {
		this.identifier = identifier;
		millis = counters.clone();
	}

	public StateTracker(String identifier) {
		this(identifier, new long[State.values().length]);
	}

	public State getState() {
		return currentState;
	}

	public State setState(State state) {
		return setState(state, System.currentTimeMillis());
	}

	public State setState(State newState, long now) {
		State prevState = currentState;
		if (prevState != null) {
			long duration = now - currentStateStarted;
			millis[prevState.ordinal()] += duration;
			total += duration;
		}
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

	public long millisTotal() {
		return total;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("StateTracker(");
		sb.append("id='").append(identifier).append("'");
		for (var m : StateTrackerMetric.METRICS) {
			String name = m.name();
			if (name.startsWith("monet."))
				name = name.substring(6);
			long value = m.extract(this);
			sb.append(", ").append(name).append("=").append(value);
		}
		sb.append(")");
		return sb.toString();
	}
}
