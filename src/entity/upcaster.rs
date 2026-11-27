use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;

use serde::{de::DeserializeOwned, Serialize};

use super::{BitcodePayloadCodec, EventRecord, EventRecordError};

/// A stateless, pure transformation that converts an event payload from one version to another.
///
/// Upcasters are plain structs with function pointers — no traits, no boxing, no dynamic dispatch.
/// They are returned as static slices (`&'static [EventUpcaster]`) for zero allocation overhead.
pub struct EventUpcaster {
    pub event_type: &'static str,
    pub from_version: u64,
    pub to_version: u64,
    pub transform: fn(event: &EventRecord) -> Result<Vec<u8>, UpcastError>,
}

/// Error returned when an upcaster chain cannot make safe forward progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpcastError {
    SameVersionTransition {
        event_type: String,
        version: u64,
    },
    BackwardTransition {
        event_type: String,
        from: u64,
        to: u64,
    },
    CycleDetected {
        event_type: String,
        version: u64,
    },
    PayloadTransform {
        event_type: String,
        from: u64,
        to: u64,
        source: EventRecordError,
    },
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpcastError::SameVersionTransition {
                event_type,
                version,
            } => write!(
                f,
                "upcaster for event {event_type} does not advance version {version}"
            ),
            UpcastError::BackwardTransition {
                event_type,
                from,
                to,
            } => write!(
                f,
                "upcaster for event {event_type} regresses version from {from} to {to}"
            ),
            UpcastError::CycleDetected {
                event_type,
                version,
            } => write!(
                f,
                "upcaster chain for event {event_type} cycles back to version {version}"
            ),
            UpcastError::PayloadTransform {
                event_type,
                from,
                to,
                source,
            } => write!(
                f,
                "failed to upcast event {event_type} from version {from} to {to}: {source}"
            ),
        }
    }
}

impl std::error::Error for UpcastError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UpcastError::PayloadTransform { source, .. } => Some(source),
            UpcastError::SameVersionTransition { .. }
            | UpcastError::BackwardTransition { .. }
            | UpcastError::CycleDetected { .. } => None,
        }
    }
}

/// Decode an event payload, transform it with typed Rust values, and encode the result.
pub fn upcast_payload<From, To>(
    event: &EventRecord,
    to_version: u64,
    transform: fn(From) -> To,
) -> Result<Vec<u8>, UpcastError>
where
    From: DeserializeOwned,
    To: Serialize,
{
    let decoded = event
        .decode::<From>()
        .map_err(|source| UpcastError::PayloadTransform {
            event_type: event.event_name.clone(),
            from: event.event_version,
            to: to_version,
            source,
        })?;

    let transformed = transform(decoded);
    BitcodePayloadCodec::encode(&transformed).map_err(|source| UpcastError::PayloadTransform {
        event_type: event.event_name.clone(),
        from: event.event_version,
        to: to_version,
        source: EventRecordError::encode(source),
    })
}

/// Apply upcasters to a list of events. Chains automatically (v1->v2->v3).
pub fn upcast_events(
    events: Vec<EventRecord>,
    upcasters: &[EventUpcaster],
) -> Result<Vec<EventRecord>, UpcastError> {
    events
        .into_iter()
        .map(|event| upcast_one(event, upcasters))
        .collect()
}

/// Whether any registered upcaster applies to `event` at its current version.
fn upcaster_applies(upcasters: &[EventUpcaster], event: &EventRecord) -> bool {
    upcasters
        .iter()
        .any(|u| u.event_type == event.event_name && u.from_version == event.event_version)
}

/// Upcast a borrowed event view for replay, cloning only the events an
/// upcaster actually rewrites.
///
/// Returns `Ok(None)` when no upcaster applies to any event (including when
/// `upcasters` is empty), so callers replay the borrowed events directly —
/// the no-op path allocates nothing. When at least one event matches, the
/// returned view borrows unmatched events and owns the upcasted ones.
///
/// `events` must be a cheaply re-runnable iterator (`Clone`): it is scanned
/// once to detect the no-op case and once more to build the view.
pub fn upcast_events_for_replay<'a, I>(
    events: I,
    upcasters: &[EventUpcaster],
) -> Result<Option<Vec<Cow<'a, EventRecord>>>, UpcastError>
where
    I: Iterator<Item = &'a EventRecord> + Clone,
{
    if upcasters.is_empty()
        || !events
            .clone()
            .any(|event| upcaster_applies(upcasters, event))
    {
        return Ok(None);
    }

    events
        .map(|event| {
            if upcaster_applies(upcasters, event) {
                upcast_one(event.clone(), upcasters).map(Cow::Owned)
            } else {
                Ok(Cow::Borrowed(event))
            }
        })
        .collect::<Result<_, _>>()
        .map(Some)
}

fn upcast_one(
    mut event: EventRecord,
    upcasters: &[EventUpcaster],
) -> Result<EventRecord, UpcastError> {
    // Cycle tracking is only needed once a transform applies; the common case
    // (no matching upcaster) allocates nothing.
    let mut seen_versions: Option<HashSet<u64>> = None;

    while let Some(u) = upcasters
        .iter()
        .find(|u| u.event_type == event.event_name && u.from_version == event.event_version)
    {
        if u.to_version == event.event_version {
            return Err(UpcastError::SameVersionTransition {
                event_type: event.event_name.clone(),
                version: event.event_version,
            });
        }
        let seen = seen_versions.get_or_insert_with(|| HashSet::from([event.event_version]));
        if seen.contains(&u.to_version) {
            return Err(UpcastError::CycleDetected {
                event_type: event.event_name.clone(),
                version: u.to_version,
            });
        }
        if u.to_version < event.event_version {
            return Err(UpcastError::BackwardTransition {
                event_type: event.event_name.clone(),
                from: event.event_version,
                to: u.to_version,
            });
        }

        let next_version = u.to_version;
        event.payload = (u.transform)(&event)?;
        event.event_version = next_version;
        seen.insert(next_version);
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_payload<T: Serialize>(value: &T) -> Vec<u8> {
        BitcodePayloadCodec::encode(value).unwrap()
    }

    fn add_default_priority((id, task): (String, String)) -> (String, String, u8) {
        (id, task, 0)
    }

    fn add_empty_due_date(
        (id, task, priority): (String, String, u8),
    ) -> (String, String, u8, String) {
        (id, task, priority, String::new())
    }

    fn upcast_test_event_v1_v2(event: &EventRecord) -> Result<Vec<u8>, UpcastError> {
        upcast_payload::<(String, String), (String, String, u8)>(event, 2, add_default_priority)
    }

    fn upcast_test_event_v2_v3(event: &EventRecord) -> Result<Vec<u8>, UpcastError> {
        upcast_payload::<(String, String, u8), (String, String, u8, String)>(
            event,
            3,
            add_empty_due_date,
        )
    }

    fn passthrough(event: &EventRecord) -> Result<Vec<u8>, UpcastError> {
        Ok(event.payload.clone())
    }

    #[test]
    fn no_upcasters_leaves_events_unchanged() {
        let event = EventRecord::new("TestEvent", vec![1, 2, 3], 1);
        let events = upcast_events(vec![event.clone()], &[]).unwrap();
        assert_eq!(events[0].payload, vec![1, 2, 3]);
        assert_eq!(events[0].event_version, 1);
    }

    #[test]
    fn chained_upcasters_v1_to_v3() {
        let event = EventRecord::new(
            "TestEvent",
            event_payload(&("id".to_string(), "task".to_string())),
            1,
        );
        let upcasters = [
            EventUpcaster {
                event_type: "TestEvent",
                from_version: 1,
                to_version: 2,
                transform: upcast_test_event_v1_v2,
            },
            EventUpcaster {
                event_type: "TestEvent",
                from_version: 2,
                to_version: 3,
                transform: upcast_test_event_v2_v3,
            },
        ];
        let events = upcast_events(vec![event], &upcasters).unwrap();
        assert_eq!(
            events[0].decode::<(String, String, u8, String)>().unwrap(),
            ("id".to_string(), "task".to_string(), 0, String::new())
        );
        assert_eq!(events[0].event_version, 3);
    }

    #[test]
    fn mixed_events_some_upcasted_some_not() {
        let events = vec![
            EventRecord::new(
                "A",
                event_payload(&("id".to_string(), "task".to_string())),
                1,
            ),
            EventRecord::new("B", vec![20], 1),
            EventRecord::new_versioned(
                "A",
                event_payload(&("id".to_string(), "task".to_string(), 99u8)),
                3,
                2,
            ),
        ];
        let upcasters = [EventUpcaster {
            event_type: "A",
            from_version: 1,
            to_version: 2,
            transform: upcast_test_event_v1_v2,
        }];
        let result = upcast_events(events, &upcasters).unwrap();
        // First A: upcasted from v1 to v2
        assert_eq!(
            result[0].decode::<(String, String, u8)>().unwrap(),
            ("id".to_string(), "task".to_string(), 0)
        );
        assert_eq!(result[0].event_version, 2);
        // B: untouched
        assert_eq!(result[1].payload, vec![20]);
        assert_eq!(result[1].event_version, 1);
        // Second A already at v2: untouched
        assert_eq!(
            result[2].decode::<(String, String, u8)>().unwrap(),
            ("id".to_string(), "task".to_string(), 99)
        );
        assert_eq!(result[2].event_version, 2);
    }

    #[test]
    fn upcast_for_replay_is_a_no_op_when_no_upcaster_matches() {
        // Every event is already at its current version, so even with an
        // upcaster registered the replay view is `None`: callers replay the
        // borrowed history directly and nothing is cloned or transformed.
        let events = [
            EventRecord::new_versioned("TestEvent", vec![1], 1, 2),
            EventRecord::new("OtherEvent", vec![2], 1),
        ];
        let upcasters = [EventUpcaster {
            event_type: "TestEvent",
            from_version: 1,
            to_version: 2,
            transform: |_| panic!("transform must not run on the no-op path"),
        }];

        assert!(upcast_events_for_replay(events.iter(), &upcasters)
            .unwrap()
            .is_none());
        assert!(upcast_events_for_replay(events.iter(), &[])
            .unwrap()
            .is_none());
    }

    #[test]
    fn upcast_for_replay_clones_only_matching_events() {
        let events = [
            EventRecord::new(
                "TestEvent",
                event_payload(&("id".to_string(), "task".to_string())),
                1,
            ),
            EventRecord::new("OtherEvent", vec![7], 1),
        ];
        let upcasters = [EventUpcaster {
            event_type: "TestEvent",
            from_version: 1,
            to_version: 2,
            transform: upcast_test_event_v1_v2,
        }];

        let view = upcast_events_for_replay(events.iter(), &upcasters)
            .unwrap()
            .expect("a matching event must produce an upcasted view");

        assert!(
            matches!(view[0], Cow::Owned(_)),
            "the rewritten event is owned"
        );
        assert_eq!(view[0].event_version, 2);
        assert!(
            matches!(view[1], Cow::Borrowed(_)),
            "a non-matching event is borrowed, not cloned"
        );
        assert_eq!(view[1].payload, vec![7]);
    }

    #[test]
    fn upcast_events_returns_payload_error_when_decode_fails() {
        let event = EventRecord::new("A", vec![10], 1);
        let upcasters = [EventUpcaster {
            event_type: "A",
            from_version: 1,
            to_version: 2,
            transform: upcast_test_event_v1_v2,
        }];

        let err = upcast_events(vec![event], &upcasters).unwrap_err();

        match err {
            UpcastError::PayloadTransform {
                event_type,
                from,
                to,
                ..
            } => {
                assert_eq!(event_type, "A");
                assert_eq!(from, 1);
                assert_eq!(to, 2);
            }
            other => panic!("expected payload transform error, got {other:?}"),
        }
    }

    #[test]
    fn payload_transform_error_exposes_source() {
        let source = EventRecordError {
            message: "decode failed".to_string(),
        };
        let err = UpcastError::PayloadTransform {
            event_type: "A".to_string(),
            from: 1,
            to: 2,
            source: source.clone(),
        };

        let chained = std::error::Error::source(&err).unwrap();

        assert_eq!(chained.to_string(), source.to_string());
    }

    #[test]
    fn upcast_events_rejects_same_version_transition() {
        let event = EventRecord::new("A", vec![10], 1);
        let upcasters = [EventUpcaster {
            event_type: "A",
            from_version: 1,
            to_version: 1,
            transform: passthrough,
        }];

        let err = upcast_events(vec![event], &upcasters).unwrap_err();

        assert_eq!(
            err,
            UpcastError::SameVersionTransition {
                event_type: "A".to_string(),
                version: 1
            }
        );
    }

    #[test]
    fn upcast_events_rejects_backward_transition() {
        let event = EventRecord::new_versioned("A", vec![10], 1, 3);
        let upcasters = [EventUpcaster {
            event_type: "A",
            from_version: 3,
            to_version: 2,
            transform: passthrough,
        }];

        let err = upcast_events(vec![event], &upcasters).unwrap_err();

        assert_eq!(
            err,
            UpcastError::BackwardTransition {
                event_type: "A".to_string(),
                from: 3,
                to: 2
            }
        );
    }

    #[test]
    fn upcast_events_rejects_cycles() {
        let event = EventRecord::new("A", vec![10], 1);
        let upcasters = [
            EventUpcaster {
                event_type: "A",
                from_version: 1,
                to_version: 2,
                transform: passthrough,
            },
            EventUpcaster {
                event_type: "A",
                from_version: 2,
                to_version: 1,
                transform: passthrough,
            },
        ];

        let err = upcast_events(vec![event], &upcasters).unwrap_err();

        assert_eq!(
            err,
            UpcastError::CycleDetected {
                event_type: "A".to_string(),
                version: 1
            }
        );
    }
}
