//! Replaying a [`Recording`] on a fresh host and finding where it diverges.

use super::*;

/// What diverged.
#[derive(Debug, Clone, PartialEq)]
pub enum DivergenceKind {
    /// State hash after a tick differs.
    StateHash {
        /// Recorded.
        expected: u64,
        /// Replayed.
        actual: u64,
    },
    /// The advance simulated different ticks.
    Ticks {
        /// Recorded ticks.
        expected: Vec<u64>,
        /// Replayed ticks.
        actual: Vec<u64>,
    },
    /// An input was accepted/rejected differently.
    InputOutcome {
        /// Recorded.
        expected: PushOutcome,
        /// Replayed.
        actual: PushOutcome,
    },
    /// A join/reconnect resolved differently.
    Join {
        /// Recorded.
        expected: JoinResult,
        /// Replayed.
        actual: JoinResult,
    },
}

/// First point where a replay departed from its recording.
#[derive(Debug, Clone, PartialEq)]
pub struct Divergence {
    /// Index into [`Recording::entries`].
    pub entry_index: usize,
    /// Tick at which it was detected (the diverging tick for hashes).
    pub tick: u64,
    /// What differed.
    pub kind: DivergenceKind,
    /// Last matching tick (0 = none).
    pub last_good_tick: u64,
    /// Stimuli applied since the last matching advance (most likely cause).
    pub suspects: Vec<String>,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DivergenceKind::StateHash { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): state hash {actual:016x} != recorded {expected:016x}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::Ticks { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): simulated ticks {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::InputOutcome { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): input outcome {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::Join { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): join {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
            )?,
        }
        write!(f, "\n  last matching tick: {}", self.last_good_tick)?;
        if !self.suspects.is_empty() {
            write!(f, "\n  stimuli since then:")?;
            for s in &self.suspects {
                write!(f, "\n    {s}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for Divergence {}

/// Successful replay summary.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaySummary {
    /// Entries applied.
    pub entries: usize,
    /// Ticks simulated and verified.
    pub ticks: u64,
    /// Hash after the last tick.
    pub final_hash: u64,
    /// Facts the zone emitted, in order (a recording without a fact queue
    /// emits none).
    pub facts: Vec<Fact>,
}

/// The clock a replay runs on: exactly the recorded readings.
pub(super) struct ReplayClock(Mutex<(Duration, u64)>);

impl ReplayClock {
    pub(super) fn set(&self, at: Duration, unix_ms: u64) {
        *self.0.lock().expect("replay clock") = (at, unix_ms);
    }
}

impl Clock for ReplayClock {
    fn now(&self) -> Duration {
        self.0.lock().expect("replay clock").0
    }

    fn unix_ms(&self) -> u64 {
        self.0.lock().expect("replay clock").1
    }
}

/// Replay `recording` through a fresh host on its recorded clock readings
/// and verify every join and input outcome and per-tick state hash.
pub fn replay<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
) -> Result<ReplaySummary, Divergence> {
    replay_with::<Z>(recording, zone_config, |_| {})
}

/// [`replay`] with a callback after each entry (inspection, tooling).
pub fn replay_with<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
    mut inspect: impl FnMut(&ZoneHost<Z>),
) -> Result<ReplaySummary, Divergence> {
    let clock = Arc::new(ReplayClock(Mutex::new((
        recording.created_at,
        recording.created_unix_ms,
    ))));
    let mut issued = 0usize;
    let mut host = ZoneHost::<Z>::new(
        recording.zone_id.clone(),
        recording.config.clone(),
        clock.clone(),
        zone_config,
    )
    .with_token_source(move || {
        issued += 1;
        token_label(issued - 1)
    })
    .with_transfer_issuer(|_: &HandoffRequest| Some("replay-transfer".into()));
    host.set_tick_hashing(true);
    let mut script = None;
    if let Some(capacity) = recording.fact_capacity {
        let (queue, s) = FactQueue::scripted(capacity);
        host = host.with_facts(queue.with_journal());
        script = Some(s);
    }
    let mut last_good_tick = 0u64;
    let mut suspects: Vec<String> = Vec::new();
    let mut last_hash = 0u64;
    let mut ticks = 0u64;
    let mut facts = Vec::new();
    for (index, entry) in recording.entries.iter().enumerate() {
        clock.set(entry.at, entry.unix_ms);
        if let (Some(script), Observed::Advance { settled, .. }) = (&script, &entry.observed) {
            for (tick, s) in settled {
                script.push(*tick, s.clone());
            }
        }
        let outcome = host.apply(entry.stimulus.clone());
        host.drain_outbound();
        facts.extend(host.take_emitted_facts());
        let diverged =
            |tick: u64, kind: DivergenceKind, last_good_tick: u64, suspects: &[String]| {
                let mut suspects = suspects.to_vec();
                suspects.push(entry.to_string());
                Divergence {
                    entry_index: index,
                    tick,
                    kind,
                    last_good_tick,
                    suspects,
                }
            };
        match (&entry.observed, Observed::of(&outcome)) {
            (Observed::Join(expected), Observed::Join(actual)) if *expected != actual => {
                let kind = DivergenceKind::Join {
                    expected: expected.clone(),
                    actual,
                };
                return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
            }
            (Observed::Input(expected), Observed::Input(actual)) if *expected != actual => {
                let kind = DivergenceKind::InputOutcome {
                    expected: *expected,
                    actual,
                };
                return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
            }
            (
                Observed::Advance { hashes, .. },
                Observed::Advance {
                    hashes: actual_hashes,
                    ..
                },
            ) => {
                for ((tick, expected), (_, actual)) in hashes.iter().zip(&actual_hashes) {
                    if expected != actual {
                        let kind = DivergenceKind::StateHash {
                            expected: *expected,
                            actual: *actual,
                        };
                        return Err(diverged(*tick, kind, last_good_tick, &suspects));
                    }
                    last_good_tick = *tick;
                }
                let expected: Vec<u64> = hashes.iter().map(|(t, _)| *t).collect();
                let actual: Vec<u64> = actual_hashes.iter().map(|(t, _)| *t).collect();
                if expected != actual {
                    let kind = DivergenceKind::Ticks { expected, actual };
                    return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
                }
                if let Some((_, h)) = actual_hashes.last() {
                    last_hash = *h;
                    ticks += actual_hashes.len() as u64;
                    suspects.clear();
                }
                inspect(&host);
                continue;
            }
            _ => {}
        }
        suspects.push(entry.to_string());
        inspect(&host);
    }
    Ok(ReplaySummary {
        entries: recording.entries.len(),
        ticks,
        final_hash: last_hash,
        facts,
    })
}

/// Per-tick hashes of a recording (for diffing two recordings).
pub fn tick_hashes(recording: &Recording) -> BTreeMap<u64, u64> {
    recording
        .entries
        .iter()
        .flat_map(|e| match &e.observed {
            Observed::Advance { hashes, .. } => hashes.clone(),
            _ => Vec::new(),
        })
        .collect()
}
