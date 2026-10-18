//! Per-player sequenced input buffers.
//!
//! Clients number inputs with a strictly increasing sequence starting at 1.
//! The buffer keeps inputs ordered by sequence, drops duplicates and inputs at
//! or below the last processed sequence, and is bounded (overflow drops the
//! oldest queued input so latency stays bounded). The zone consumes inputs
//! during a tick with a [`ConsumePolicy`]; the highest consumed sequence
//! becomes the **ack** sent back in every Delta so a predicting client can
//! discard confirmed inputs and replay the rest (reconciliation).
//!
//! Sanitizing (clamping/validating game input) happens before buffering via
//! [`Zone::sanitize_input`](crate::Zone::sanitize_input).

use std::collections::VecDeque;

/// How a tick consumes queued inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumePolicy {
    /// Oldest queued input only (one input per tick; lockstep-style).
    Next,
    /// Newest queued input; older queued inputs are discarded but acked.
    Latest,
    /// Every queued input in sequence order.
    Drain,
}

/// Outcome of offering an input to a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// Queued.
    Accepted,
    /// Same sequence already queued.
    Duplicate,
    /// Sequence at or below the last processed sequence.
    Stale,
    /// Queued after dropping the oldest queued input (buffer full).
    AcceptedDroppedOldest,
    /// Rejected by the zone's sanitize hook.
    Rejected,
    /// Sender is not an admitted player.
    UnknownClient,
}

/// One buffered input.
#[derive(Debug, Clone, PartialEq)]
pub struct SequencedInput<I> {
    /// Client sequence (>= 1).
    pub seq: u32,
    /// Client's predicted tick when the input was sampled (informational).
    pub client_tick: u32,
    /// Sanitized game input.
    pub input: I,
}

/// Bounded, deduplicating, ordered input queue for one player.
#[derive(Debug, Clone)]
pub struct InputBuffer<I> {
    queue: VecDeque<SequencedInput<I>>,
    capacity: usize,
    last_processed: u32,
    dropped: u64,
}

impl<I> InputBuffer<I> {
    /// Buffer holding at most `capacity` (>= 1) inputs.
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity: capacity.max(1),
            last_processed: 0,
            dropped: 0,
        }
    }

    /// Highest consumed sequence (0 = none). Sent to the client as `ack_seq`.
    pub fn last_processed(&self) -> u32 {
        self.last_processed
    }

    /// Queued input count.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// True when nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Inputs dropped by overflow so far.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Check whether `seq` would be accepted, without inserting.
    pub fn precheck(&self, seq: u32) -> Option<PushOutcome> {
        if seq <= self.last_processed {
            return Some(PushOutcome::Stale);
        }
        if self.queue.iter().any(|queued| queued.seq == seq) {
            return Some(PushOutcome::Duplicate);
        }
        None
    }

    /// Insert in sequence order.
    pub fn push(&mut self, seq: u32, client_tick: u32, input: I) -> PushOutcome {
        if let Some(outcome) = self.precheck(seq) {
            return outcome;
        }
        let position = self
            .queue
            .iter()
            .position(|queued| queued.seq > seq)
            .unwrap_or(self.queue.len());
        self.queue.insert(
            position,
            SequencedInput {
                seq,
                client_tick,
                input,
            },
        );
        if self.queue.len() > self.capacity {
            self.queue.pop_front();
            self.dropped += 1;
            return PushOutcome::AcceptedDroppedOldest;
        }
        PushOutcome::Accepted
    }

    /// Consume per `policy`, advancing the ack to the highest consumed or
    /// discarded sequence.
    pub fn take(&mut self, policy: ConsumePolicy) -> Vec<SequencedInput<I>> {
        let taken: Vec<_> = match policy {
            ConsumePolicy::Next => self.queue.pop_front().into_iter().collect(),
            ConsumePolicy::Drain => self.queue.drain(..).collect(),
            ConsumePolicy::Latest => {
                let latest = self.queue.pop_back();
                self.queue.clear();
                latest.into_iter().collect()
            }
        };
        if let Some(last) = taken.last() {
            self.last_processed = self.last_processed.max(last.seq);
        }
        taken
    }

    /// Reset the queue (keeps the ack so stale resends are still rejected).
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_orders_and_acks() {
        let mut buf = InputBuffer::new(8);
        assert_eq!(buf.push(2, 0, 'b'), PushOutcome::Accepted);
        assert_eq!(buf.push(1, 0, 'a'), PushOutcome::Accepted);
        assert_eq!(buf.push(2, 0, 'x'), PushOutcome::Duplicate);
        let next = buf.take(ConsumePolicy::Next);
        assert_eq!(next[0].input, 'a');
        assert_eq!(buf.last_processed(), 1);
        assert_eq!(buf.push(1, 0, 'z'), PushOutcome::Stale);
        assert_eq!(buf.take(ConsumePolicy::Drain).len(), 1);
        assert_eq!(buf.last_processed(), 2);
    }

    #[test]
    fn latest_discards_but_acks_older() {
        let mut buf = InputBuffer::new(8);
        for seq in 1..=5 {
            buf.push(seq, 0, seq);
        }
        let taken = buf.take(ConsumePolicy::Latest);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].input, 5);
        assert_eq!(buf.last_processed(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn bounded_drops_oldest() {
        let mut buf = InputBuffer::new(2);
        buf.push(1, 0, ());
        buf.push(2, 0, ());
        assert_eq!(buf.push(3, 0, ()), PushOutcome::AcceptedDroppedOldest);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.dropped(), 1);
        let seqs: Vec<_> = buf
            .take(ConsumePolicy::Drain)
            .iter()
            .map(|i| i.seq)
            .collect();
        assert_eq!(seqs, vec![2, 3]);
    }
}
