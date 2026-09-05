package broker

// The counters below are the only way to see the broker from outside. Each one
// answers a question you cannot answer from a log line:
//
//   - published vs publishFails: is the relay getting messages out at all?
//   - fanoutRecv: is THIS node still receiving? A node stuck at 0 while the
//     others climb has lost its subscription, and its sockets have gone quiet.
//   - redelivered: how often work is being done twice. A slow rise is normal
//     at-least-once behaviour; a spike means something is timing out.
//   - deadLettered: this should be zero. Any other number is a bug that needs
//     a person.
//
// They are methods and not exported fields so that a nil broker — single-node
// mode — answers zero instead of panicking. metrics.RegisterBroker calls them
// on every scrape.

// Published is how many events this node's relay put on the stream.
func (b *Broker) Published() int64 {
	if b == nil {
		return 0
	}
	return b.published.Load()
}

// PublishFailed is how many publishes failed. The outbox row stays unpublished
// after each one, so this counter rising is not lost data — it is delayed
// data.
func (b *Broker) PublishFailed() int64 {
	if b == nil {
		return 0
	}
	return b.publishFails.Load()
}

// FanoutReceived is how many events this node received for its own sockets.
func (b *Broker) FanoutReceived() int64 {
	if b == nil {
		return 0
	}
	return b.fanoutRecv.Load()
}

// UnreadHandled is how many events this node acknowledged on the shared
// consumer. Across all nodes these add up to the number of messages sent; on
// one node it is that node's share of the work.
func (b *Broker) UnreadHandled() int64 {
	if b == nil {
		return 0
	}
	return b.unreadDone.Load()
}

// Redelivered is how many events arrived on the shared consumer for at least
// the second time.
func (b *Broker) Redelivered() int64 {
	if b == nil {
		return 0
	}
	return b.redelivered.Load()
}

// DeadLettered is how many events gave up and went to CHAT_DLQ. Expected
// value: zero.
func (b *Broker) DeadLettered() int64 {
	if b == nil {
		return 0
	}
	return b.deadLettered.Load()
}

// Consuming reports whether this node's fan-out subscription is live: 1 or 0.
// It is a gauge, not a counter, and it is the first thing to look at when one
// node's users stop seeing messages.
func (b *Broker) Consuming() float64 {
	if b == nil || !b.consuming.Load() {
		return 0
	}
	return 1
}
