package outbox

// The relay's numbers on /metrics.
//
// Lag is the one that matters. A pending count on its own says nothing — 900
// rows could be one busy second. Nine hundred rows whose oldest has waited
// four minutes means the relay has stopped, and every message sent in those
// four minutes is sitting in Postgres waiting for somebody to notice.
//
// IsLeader is the second one. Across the cluster it must add up to exactly 1.
// Zero means nobody is draining. Two means the lock is broken and messages are
// going out of order.

// IsLeader reports whether this node currently holds the relay lock: 1 or 0.
func (r *Relay) IsLeader() float64 {
	if r == nil || !r.leader.Load() {
		return 0
	}
	return 1
}

// Published is how many outbox rows this node has drained since it started.
func (r *Relay) Published() int64 {
	if r == nil {
		return 0
	}
	return r.published.Load()
}

// Failed is how many publish attempts failed. The rows stay in the table, so
// this counter rising means delivery is late, not lost.
func (r *Relay) Failed() int64 {
	if r == nil {
		return 0
	}
	return r.failed.Load()
}

// Pending is how many outbox rows are still waiting, refreshed every few
// seconds. At rest it should be 0.
func (r *Relay) Pending() float64 {
	if r == nil {
		return 0
	}
	return float64(r.pending.Load())
}

// LagSeconds is how long the oldest waiting row has waited. 0 when nothing is
// waiting. This is the health of the whole stage in one number.
func (r *Relay) LagSeconds() float64 {
	if r == nil {
		return 0
	}
	return float64(r.lagMillis.Load()) / 1000
}
