//! The recording file: compact binary (`DGR4`, varints).

use super::*;

pub(super) const MAGIC: &[u8; 4] = b"DGR4";

pub(super) fn bad(what: &'static str) -> RecordingError {
    RecordingError::Decode(what.into())
}

pub(super) fn put_u64(w: &mut Writer, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            w.u8(byte);
            return;
        }
        w.u8(byte | 0x80);
    }
}

pub(super) fn get_u64(r: &mut Reader<'_>) -> Result<u64, RecordingError> {
    let mut v = 0u64;
    for i in 0..10 {
        let byte = r.u8()?;
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(bad("u64 varint overflow"))
}

pub(super) fn put_dur(w: &mut Writer, d: Duration) {
    put_u64(w, d.as_nanos() as u64);
}

pub(super) fn get_dur(r: &mut Reader<'_>) -> Result<Duration, RecordingError> {
    Ok(Duration::from_nanos(get_u64(r)?))
}

pub(super) fn put_notice(w: &mut Writer, n: &Notice) {
    w.string(&n.reason).string(&n.version).string(&n.notes);
}

pub(super) fn get_notice(r: &mut Reader<'_>) -> Result<Notice, RecordingError> {
    Ok(Notice {
        reason: r.string()?,
        version: r.string()?,
        notes: r.string()?,
    })
}

pub(super) fn put_join_result(w: &mut Writer, j: &JoinResult) {
    match j {
        JoinResult::Joined(c) => w.u8(0).varuint(*c),
        JoinResult::Refused(c) => w.u8(1).varuint(*c),
    };
}

pub(super) fn get_join_result(r: &mut Reader<'_>) -> Result<JoinResult, RecordingError> {
    match r.u8()? {
        0 => Ok(JoinResult::Joined(r.varuint()?)),
        1 => Ok(JoinResult::Refused(r.varuint()?)),
        _ => Err(bad("join result")),
    }
}

pub(super) const OUTCOMES: [PushOutcome; 6] = [
    PushOutcome::Accepted,
    PushOutcome::Duplicate,
    PushOutcome::Stale,
    PushOutcome::AcceptedDroppedOldest,
    PushOutcome::Rejected,
    PushOutcome::UnknownClient,
];

pub(super) fn put_settlement(w: &mut Writer, tick: u64, s: &FactSettlement) {
    put_u64(w, tick);
    w.string(&s.key).string(&s.command);
    match &s.outcome {
        FactOutcome::Committed(receipt) => {
            w.u8(0)
                .string(&receipt.payload.to_string())
                .bool(receipt.replayed);
        }
        FactOutcome::Failed { error, attempts } => {
            let (kind, msg) = match error {
                FactError::Retryable(m) => (0, m),
                FactError::Permanent(m) => (1, m),
            };
            w.u8(1).u8(kind).string(msg).varuint(*attempts);
        }
    }
}

pub(super) fn get_settlement(r: &mut Reader<'_>) -> Result<(u64, FactSettlement), RecordingError> {
    let tick = get_u64(r)?;
    let key = r.string()?;
    let command = r.string()?;
    let outcome = match r.u8()? {
        0 => {
            let payload = serde_json::from_str(&r.string()?).map_err(|_| bad("receipt json"))?;
            FactOutcome::Committed(FactReceipt {
                payload,
                replayed: r.bool()?,
            })
        }
        1 => {
            let kind = r.u8()?;
            let msg = r.string()?;
            let error = match kind {
                0 => FactError::Retryable(msg),
                1 => FactError::Permanent(msg),
                _ => return Err(bad("fact error kind")),
            };
            FactOutcome::Failed {
                error,
                attempts: r.varuint()?,
            }
        }
        _ => return Err(bad("fact outcome")),
    };
    Ok((
        tick,
        FactSettlement {
            key,
            command,
            outcome,
        },
    ))
}

pub(super) fn put_config(w: &mut Writer, c: &HostConfig) {
    w.varuint(c.tick_rate).varuint(c.patch_rate);
    match c.idle_tick_rate {
        Some(hz) => w.u8(1).varuint(hz),
        None => w.u8(0),
    };
    put_dur(w, c.dormant_after);
    w.varuint(c.max_catch_up);
    put_u64(w, c.max_players as u64);
    put_u64(w, c.max_observers as u64);
    put_u64(w, c.input_capacity as u64);
    put_dur(w, c.reconnect_grace);
    put_u64(w, c.byte_budget as u64);
    w.f32(c.default_view_radius)
        .f32(c.interest.cell_size)
        .f32(c.interest.leave_margin)
        .string(&c.content_version);
    put_u64(w, c.world_seed);
    put_dur(w, c.rewind.max_rewind);
    put_u64(w, c.rewind.max_frames as u64);
    w.u8(match c.stale_policy {
        StalePolicy::Strict => 0,
        StalePolicy::SafeRoom { roam: false } => 1,
        StalePolicy::SafeRoom { roam: true } => 2,
    });
    put_u64(w, c.boot_epoch);
    w.string(&c.config_version);
    w.bool(c.lifecycle_facts);
}

pub(super) fn put_claims(w: &mut Writer, claims: &TicketClaims) {
    w.string(&claims.principal)
        .string(&claims.zone)
        .string(&claims.nonce);
    put_u64(w, claims.expires_at);
    w.u8(match claims.role {
        Role::Player => 0,
        Role::Observer => 1,
    });
    match &claims.name {
        Some(name) => w.u8(1).string(name),
        None => w.u8(0),
    };
    w.string(&claims.extensions.to_json().to_string());
}

pub(super) fn get_claims(r: &mut Reader<'_>) -> Result<TicketClaims, RecordingError> {
    let principal = r.string()?;
    let zone = r.string()?;
    let nonce = r.string()?;
    let expires_at = get_u64(r)?;
    let role = match r.u8()? {
        0 => Role::Player,
        _ => Role::Observer,
    };
    let name = match r.u8()? {
        0 => None,
        _ => Some(r.string()?),
    };
    let extensions = serde_json::from_str(&r.string()?)
        .ok()
        .and_then(|value| crate::claims::TicketExtensions::from_json(value).ok())
        .ok_or_else(|| bad("extension claims"))?;
    Ok(TicketClaims {
        principal,
        zone,
        expires_at,
        nonce,
        role,
        name,
        extensions,
        wake: false,
    })
}

pub(super) fn get_config(r: &mut Reader<'_>) -> Result<HostConfig, RecordingError> {
    Ok(HostConfig {
        tick_rate: r.varuint()?,
        patch_rate: r.varuint()?,
        idle_tick_rate: match r.u8()? {
            0 => None,
            _ => Some(r.varuint()?),
        },
        dormant_after: get_dur(r)?,
        max_catch_up: r.varuint()?,
        max_players: get_u64(r)? as usize,
        max_observers: get_u64(r)? as usize,
        input_capacity: get_u64(r)? as usize,
        reconnect_grace: get_dur(r)?,
        byte_budget: get_u64(r)? as usize,
        default_view_radius: r.f32()?,
        // Update rates only shape Deltas, never the replayed world.
        interest: InterestConfig {
            cell_size: r.f32()?,
            leave_margin: r.f32()?,
            ..InterestConfig::default()
        },
        content_version: r.string()?,
        world_seed: get_u64(r)?,
        rewind: RewindConfig {
            max_rewind: get_dur(r)?,
            max_frames: get_u64(r)? as usize,
        },
        stale_policy: match r.u8()? {
            0 => StalePolicy::Strict,
            1 => StalePolicy::SafeRoom { roam: false },
            _ => StalePolicy::SafeRoom { roam: true },
        },
        boot_epoch: get_u64(r)?,
        config_version: r.string()?,
        lifecycle_facts: r.bool()?,
        record: false,
    })
}

pub(super) fn put_opt_string(w: &mut Writer, v: Option<&str>) {
    match v {
        Some(v) => w.u8(1).string(v),
        None => w.u8(0),
    };
}

pub(super) fn get_opt_string(r: &mut Reader<'_>) -> Result<Option<String>, RecordingError> {
    Ok(match r.u8()? {
        0 => None,
        _ => Some(r.string()?),
    })
}

pub(super) fn put_stimulus(w: &mut Writer, stimulus: &Stimulus) {
    match stimulus {
        Stimulus::Join {
            claims,
            capabilities,
            content_version,
        } => {
            w.u8(0);
            put_claims(w, claims);
            w.varuint(*capabilities).string(content_version);
        }
        Stimulus::JoinTransfer {
            transfer,
            capabilities,
            content_version,
        } => {
            w.u8(1);
            put_claims(w, &transfer.claims);
            w.string(&transfer.source_zone).string(&transfer.issuer);
            put_u64(w, transfer.generation);
            put_u64(w, transfer.departed_at_ms);
            w.bytes(&transfer.carried)
                .varuint(*capabilities)
                .string(content_version);
        }
        Stimulus::Reconnect {
            token,
            capabilities,
            content_version,
        } => {
            w.u8(2).string(token).varuint(*capabilities);
            put_opt_string(w, content_version.as_deref());
        }
        Stimulus::Disconnect { client, graceful } => {
            w.u8(3).varuint(client.0).bool(*graceful);
        }
        Stimulus::Input {
            client,
            seq,
            client_tick,
            payload,
        } => {
            w.u8(4)
                .varuint(client.0)
                .varuint(*seq)
                .varuint(*client_tick)
                .bytes(payload);
        }
        Stimulus::Content { client, version } => {
            w.u8(5).varuint(client.0).string(version);
        }
        Stimulus::Focus { client, x, y } => {
            w.u8(6).varuint(client.0).f32(*x).f32(*y);
        }
        Stimulus::Kick { principal, reason } => {
            w.u8(7).string(principal).string(reason);
        }
        Stimulus::Message { type_id, payload } => {
            w.u8(8).varuint(*type_id).bytes(payload);
        }
        Stimulus::ZoneMessage(envelope) => {
            w.u8(9)
                .string(&envelope.id)
                .string(&envelope.from)
                .string(&envelope.to);
            put_opt_string(w, envelope.principal.as_deref());
            w.varuint(envelope.type_id).bytes(&envelope.payload);
        }
        Stimulus::ChildEvent(event) => {
            w.u8(10)
                .string(&event.origin_zone)
                .string(&event.from_zone)
                .string(&event.kind)
                .bytes(&event.payload);
            put_u64(w, event.origin_tick);
            w.string(&event.content_version).u8(event.levels);
        }
        Stimulus::RequestFreeze(notice) => {
            w.u8(11);
            put_notice(w, notice);
        }
        Stimulus::CancelFreeze(notice) => {
            w.u8(12);
            put_notice(w, notice);
        }
        Stimulus::Resume(notice) => {
            w.u8(13);
            put_notice(w, notice);
        }
        Stimulus::ContentVersion(version) => {
            w.u8(14).string(version);
        }
        Stimulus::ConfigVersion(version) => {
            w.u8(15).string(version);
        }
        Stimulus::Reload(player) => {
            w.u8(16).varuint(player.0);
        }
        Stimulus::ReloadStale => {
            w.u8(17);
        }
        Stimulus::KickPlayer { player, message } => {
            w.u8(18).varuint(player.0).string(message);
        }
        Stimulus::Handoff {
            player,
            target_zone,
            ticket,
        } => {
            w.u8(19)
                .varuint(player.0)
                .string(target_zone)
                .string(ticket);
        }
        Stimulus::Restore {
            state,
            saved_version,
        } => {
            w.u8(20).bytes(state).string(saved_version);
        }
        Stimulus::Dispose => {
            w.u8(21);
        }
        Stimulus::Advance => {
            w.u8(22);
        }
    }
}

pub(super) fn get_stimulus(r: &mut Reader<'_>) -> Result<Stimulus, RecordingError> {
    Ok(match r.u8()? {
        0 => Stimulus::Join {
            claims: get_claims(r)?,
            capabilities: r.varuint()?,
            content_version: r.string()?,
        },
        1 => {
            let claims = get_claims(r)?;
            let source_zone = r.string()?;
            let issuer = r.string()?;
            let generation = get_u64(r)?;
            let departed_at_ms = get_u64(r)?;
            let carried = r.bytes()?.to_vec();
            Stimulus::JoinTransfer {
                transfer: Transfer {
                    claims,
                    source_zone,
                    issuer,
                    generation,
                    carried,
                    departed_at_ms,
                },
                capabilities: r.varuint()?,
                content_version: r.string()?,
            }
        }
        2 => Stimulus::Reconnect {
            token: r.string()?,
            capabilities: r.varuint()?,
            content_version: get_opt_string(r)?,
        },
        3 => Stimulus::Disconnect {
            client: ClientId(r.varuint()?),
            graceful: r.bool()?,
        },
        4 => Stimulus::Input {
            client: ClientId(r.varuint()?),
            seq: r.varuint()?,
            client_tick: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        },
        5 => Stimulus::Content {
            client: ClientId(r.varuint()?),
            version: r.string()?,
        },
        6 => Stimulus::Focus {
            client: ClientId(r.varuint()?),
            x: r.f32()?,
            y: r.f32()?,
        },
        7 => Stimulus::Kick {
            principal: r.string()?,
            reason: r.string()?,
        },
        8 => Stimulus::Message {
            type_id: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        },
        9 => Stimulus::ZoneMessage(ZoneEnvelope {
            id: r.string()?,
            from: r.string()?,
            to: r.string()?,
            principal: get_opt_string(r)?,
            type_id: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        }),
        10 => Stimulus::ChildEvent(ChildEvent {
            origin_zone: r.string()?,
            from_zone: r.string()?,
            kind: r.string()?,
            payload: r.bytes()?.to_vec(),
            origin_tick: get_u64(r)?,
            content_version: r.string()?,
            levels: r.u8()?,
        }),
        11 => Stimulus::RequestFreeze(get_notice(r)?),
        12 => Stimulus::CancelFreeze(get_notice(r)?),
        13 => Stimulus::Resume(get_notice(r)?),
        14 => Stimulus::ContentVersion(r.string()?),
        15 => Stimulus::ConfigVersion(r.string()?),
        16 => Stimulus::Reload(PlayerId(r.varuint()?)),
        17 => Stimulus::ReloadStale,
        18 => Stimulus::KickPlayer {
            player: PlayerId(r.varuint()?),
            message: r.string()?,
        },
        19 => Stimulus::Handoff {
            player: PlayerId(r.varuint()?),
            target_zone: r.string()?,
            ticket: r.string()?,
        },
        20 => Stimulus::Restore {
            state: r.bytes()?.to_vec(),
            saved_version: r.string()?,
        },
        21 => Stimulus::Dispose,
        22 => Stimulus::Advance,
        _ => return Err(bad("stimulus tag")),
    })
}

pub(super) fn put_observed(w: &mut Writer, observed: &Observed) {
    match observed {
        Observed::Nothing => {
            w.u8(0);
        }
        Observed::Join(result) => {
            w.u8(1);
            put_join_result(w, result);
        }
        Observed::Input(outcome) => {
            let code = OUTCOMES.iter().position(|o| o == outcome).unwrap_or(0) as u8;
            w.u8(2).u8(code);
        }
        Observed::Advance { settled, hashes } => {
            w.u8(3);
            put_u64(w, settled.len() as u64);
            for (tick, s) in settled {
                put_settlement(w, *tick, s);
            }
            put_u64(w, hashes.len() as u64);
            if let Some((first, _)) = hashes.first() {
                put_u64(w, *first);
            }
            for (_, hash) in hashes {
                w.raw(&hash.to_le_bytes());
            }
        }
    }
}

pub(super) fn get_observed(r: &mut Reader<'_>) -> Result<Observed, RecordingError> {
    Ok(match r.u8()? {
        0 => Observed::Nothing,
        1 => Observed::Join(get_join_result(r)?),
        2 => Observed::Input(*OUTCOMES.get(r.u8()? as usize).ok_or(bad("input outcome"))?),
        3 => {
            let n = get_u64(r)? as usize;
            let mut settled = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                settled.push(get_settlement(r)?);
            }
            let n = get_u64(r)?;
            let mut hashes = Vec::with_capacity((n as usize).min(1 << 16));
            if n > 0 {
                let first = get_u64(r)?;
                for i in 0..n {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(r.raw(8)?);
                    hashes.push((first + i, u64::from_le_bytes(b)));
                }
            }
            Observed::Advance { settled, hashes }
        }
        _ => return Err(bad("observed tag")),
    })
}

impl Recording {
    /// Compact binary encoding:
    /// `"DGR4" ‖ zone_id ‖ config ‖ created_at ‖ created_unix_ms ‖
    /// fact_capacity? ‖ varuint(entry count) ‖ entries`, each entry
    /// `at ‖ unix_ms ‖ stimulus ‖ observed`. Integers are LEB128 varints,
    /// durations are nanosecond varints, clock readings are delta-coded
    /// against the previous entry (both clocks never go backwards in a
    /// recording: the host clamps them), hashes are 8 raw bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(MAGIC).string(&self.zone_id);
        put_config(&mut w, &self.config);
        put_dur(&mut w, self.created_at);
        put_u64(&mut w, self.created_unix_ms);
        match self.fact_capacity {
            Some(c) => {
                w.u8(1);
                put_u64(&mut w, c as u64);
            }
            None => {
                w.u8(0);
            }
        }
        put_u64(&mut w, self.entries.len() as u64);
        let (mut at, mut unix_ms) = (self.created_at, self.created_unix_ms);
        for entry in &self.entries {
            put_dur(&mut w, entry.at.saturating_sub(at));
            put_u64(&mut w, entry.unix_ms.saturating_sub(unix_ms));
            (at, unix_ms) = (entry.at, entry.unix_ms);
            put_stimulus(&mut w, &entry.stimulus);
            put_observed(&mut w, &entry.observed);
        }
        w.into_bytes()
    }

    /// Decode [`encode`](Self::encode) output.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordingError> {
        let mut r = Reader::new(bytes);
        if r.raw(4).map_err(|_| RecordingError::BadMagic)? != MAGIC {
            return Err(RecordingError::BadMagic);
        }
        let zone_id = r.string()?;
        let config = get_config(&mut r)?;
        let created_at = get_dur(&mut r)?;
        let created_unix_ms = get_u64(&mut r)?;
        let fact_capacity = match r.u8()? {
            0 => None,
            _ => Some(get_u64(&mut r)? as usize),
        };
        let count = get_u64(&mut r)? as usize;
        let mut entries = Vec::with_capacity(count.min(1 << 20));
        let (mut at, mut unix_ms) = (created_at, created_unix_ms);
        for _ in 0..count {
            at += get_dur(&mut r)?;
            unix_ms = unix_ms
                .checked_add(get_u64(&mut r)?)
                .ok_or(bad("wall clock overflow"))?;
            entries.push(Entry {
                at,
                unix_ms,
                stimulus: get_stimulus(&mut r)?,
                observed: get_observed(&mut r)?,
            });
        }
        r.finish()?;
        Ok(Self {
            zone_id,
            config,
            created_at,
            created_unix_ms,
            fact_capacity,
            entries,
        })
    }

    /// Write [`encode`](Self::encode) output to `path`.
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        std::fs::write(path, self.encode())
    }

    /// Read a file written by [`save`](Self::save).
    pub fn load(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::decode(&bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Simulated ticks in the recording.
    pub fn tick_count(&self) -> usize {
        self.entries
            .iter()
            .map(|e| match &e.observed {
                Observed::Advance { hashes, .. } => hashes.len(),
                _ => 0,
            })
            .sum()
    }
}
