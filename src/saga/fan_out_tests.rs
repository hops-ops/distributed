//! Fan-out steps: N commands to N cells decided at run time, per-member
//! timeouts, compensation of only the applied members, and replay.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{
    Saga, SagaAction, SagaCommand, SagaDefinition, SagaError, SagaMessage, SagaOutcome, SagaPhase,
    SagaReply, SagaRunner, SagaStatus, SagaStep, MAX_SAGA_FAN_OUT, META_MEMBER, SAGA_TIMEOUT,
};
use crate::outbox::OutboxMessage;
use crate::{InMemoryRepository, ManualClock};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct MatchStart {
    parties: Vec<String>,
    /// Reply payload of each reserved member, folded by `on_step_succeeded`.
    reserved: BTreeMap<usize, String>,
    confirmed: bool,
}

struct StartMatch;

impl SagaDefinition for StartMatch {
    const SAGA_TYPE: &'static str = "test.match_start";
    const REPLY_TO: &'static str = "match-start-saga";
    type Data = MatchStart;

    fn steps() -> &'static [SagaStep] {
        const STEPS: &[SagaStep] = &[
            SagaStep::new("reserve", Duration::from_secs(5)),
            SagaStep::new("confirm", Duration::from_secs(5)),
        ];
        STEPS
    }

    fn action(step: &str, saga_id: &str, data: &MatchStart) -> Result<SagaAction, SagaError> {
        match step {
            "reserve" => Ok(SagaAction::fan_out(
                data.parties
                    .iter()
                    .map(|party| {
                        SagaCommand::json(format!("party:{party}"), "ticket.reserve", &saga_id)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            "confirm" => Ok(SagaCommand::json("match", "match.confirm", &saga_id)?.into()),
            other => Err(SagaError::Definition(format!("unknown step {other}"))),
        }
    }

    fn compensation(
        step: &str,
        member: usize,
        saga_id: &str,
        data: &MatchStart,
    ) -> Result<Option<SagaCommand>, SagaError> {
        match step {
            "reserve" => SagaCommand::json(
                format!("party:{}", data.parties[member]),
                "ticket.release",
                &saga_id,
            )
            .map(Some),
            _ => Ok(None),
        }
    }

    fn on_step_succeeded(step: &str, member: usize, data: &mut MatchStart, output: &[u8]) {
        match step {
            "reserve" => {
                data.reserved
                    .insert(member, String::from_utf8_lossy(output).into_owned());
            }
            "confirm" => data.confirmed = true,
            _ => {}
        }
    }
}

const T0: Duration = Duration::from_secs(1_700_000_000);

fn now() -> SystemTime {
    SystemTime::UNIX_EPOCH + T0
}

fn data(n: usize) -> MatchStart {
    MatchStart {
        parties: (0..n).map(|index| format!("p{index}")).collect(),
        ..MatchStart::default()
    }
}

fn commands(messages: &[OutboxMessage]) -> Vec<&OutboxMessage> {
    messages
        .iter()
        .filter(|message| message.event_type != SAGA_TIMEOUT)
        .collect()
}

fn timers(messages: &[OutboxMessage]) -> Vec<&OutboxMessage> {
    messages
        .iter()
        .filter(|message| message.event_type == SAGA_TIMEOUT)
        .collect()
}

fn succeeded(command: &OutboxMessage, output: &str) -> SagaMessage {
    let reply = SagaReply::from_outbox(command)
        .unwrap()
        .succeeded(output.as_bytes().to_vec())
        .unwrap();
    SagaMessage::from_outbox(&reply).unwrap()
}

fn failed(command: &OutboxMessage, reason: &str) -> SagaMessage {
    let reply = SagaReply::from_outbox(command)
        .unwrap()
        .failed(reason)
        .unwrap();
    SagaMessage::from_outbox(&reply).unwrap()
}

fn timed_out(timer: &OutboxMessage) -> SagaMessage {
    SagaMessage::from_outbox(timer).unwrap()
}

fn member(message: &OutboxMessage) -> usize {
    message.metadata[META_MEMBER].parse().unwrap()
}

/// Drive a compensation stack to the end, acknowledging each compensation
/// and returning the members compensated, in order.
fn drain_compensations(
    saga: &mut Saga<StartMatch>,
    mut messages: Vec<OutboxMessage>,
) -> Vec<usize> {
    let mut compensated = Vec::new();
    while let Some(command) = commands(&messages).first().copied().cloned() {
        assert_eq!(command.event_type, "ticket.release");
        compensated.push(member(&command));
        messages = saga
            .handle(&succeeded(&command, "released"), now())
            .unwrap()
            .messages;
    }
    compensated
}

#[test]
fn fan_out_of_one_to_sixteen_members_completes() {
    for n in 1..=16 {
        let mut saga = Saga::<StartMatch>::new();
        let started = saga.start("m1", data(n), now()).unwrap();
        let sent = commands(&started.messages);
        assert_eq!(sent.len(), n, "n={n}");
        assert_eq!(timers(&started.messages).len(), n, "one timer per member");
        let cursor = saga.cursor().unwrap();
        assert_eq!(cursor.fan_out, n as u32);
        for (index, command) in sent.iter().enumerate() {
            assert_eq!(
                command.id(),
                format!("test.match_start:m1:reserve:action:1:{index}"),
                "deterministic command id per (step, member)"
            );
            assert_eq!(member(command), index);
            assert_eq!(
                command.destination.as_deref(),
                Some(&*format!("party:p{index}"))
            );
        }
        // Replies arrive in any order; the step waits for all of them.
        let mut next = Vec::new();
        for command in sent.iter().rev() {
            assert_eq!(saga.status(), SagaStatus::Running);
            assert!(saga.cursor().is_some_and(|c| c.step == 0));
            next = saga
                .handle(
                    &succeeded(command, &format!("ticket-{}", member(command))),
                    now(),
                )
                .unwrap()
                .messages;
        }
        let confirm = commands(&next);
        assert_eq!(confirm.len(), 1);
        assert_eq!(confirm[0].event_type, "match.confirm");
        assert!(!confirm[0].metadata.contains_key(META_MEMBER));
        saga.handle(&succeeded(confirm[0], "ok"), now()).unwrap();
        assert_eq!(saga.status(), SagaStatus::Completed);
        assert_eq!(saga.data().reserved.len(), n);
        assert_eq!(saga.data().reserved[&0], "ticket-0");
        assert!(saga.data().confirmed);
    }
}

#[test]
fn one_rejection_compensates_only_the_applied_members() {
    let mut saga = Saga::<StartMatch>::new();
    let started = saga.start("m2", data(5), now()).unwrap();
    let sent: Vec<OutboxMessage> = commands(&started.messages).into_iter().cloned().collect();
    for index in [0, 1] {
        let transition = saga.handle(&succeeded(&sent[index], "t"), now()).unwrap();
        assert!(transition.messages.is_empty());
    }
    // A rejection does not settle the step while member 3 and 4 are pending:
    // their outcome must be known before choosing what to compensate.
    let rejected = saga.handle(&failed(&sent[2], "party left"), now()).unwrap();
    assert!(rejected.messages.is_empty());
    assert_eq!(saga.status(), SagaStatus::Running);
    saga.handle(&succeeded(&sent[3], "t"), now()).unwrap();
    let settled = saga.handle(&succeeded(&sent[4], "t"), now()).unwrap();
    assert_eq!(saga.status(), SagaStatus::Compensating);
    let failure = saga.failure().unwrap();
    assert_eq!(failure.step, "reserve");
    assert!(failure.reason.contains("party left"));
    assert!(!failure.timed_out);
    assert_eq!(
        saga.applied_members("reserve")
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        [0, 1, 3, 4]
    );

    let compensated = drain_compensations(&mut saga, settled.messages);
    assert_eq!(compensated, [4, 3, 1, 0], "never the member that rejected");
    assert_eq!(saga.status(), SagaStatus::Compensated);
}

#[test]
fn members_time_out_individually_and_late_replies_are_ignored() {
    let mut saga = Saga::<StartMatch>::new();
    let started = saga.start("m3", data(3), now()).unwrap();
    let sent: Vec<OutboxMessage> = commands(&started.messages).into_iter().cloned().collect();
    let member_timers: Vec<OutboxMessage> =
        timers(&started.messages).into_iter().cloned().collect();
    assert_eq!(member(&member_timers[1]), 1);
    assert_eq!(
        member_timers[1].deliver_at,
        Some(now() + Duration::from_secs(5))
    );

    saga.handle(&succeeded(&sent[0], "t"), now()).unwrap();
    let timeout = timed_out(&member_timers[1]);
    assert_eq!(
        saga.handle(&timeout, now()).unwrap().outcome,
        SagaOutcome::Applied
    );
    assert_eq!(
        saga.handle(&timeout, now()).unwrap().outcome,
        SagaOutcome::Duplicate
    );
    // The participant answers after its timer fired: ignored.
    assert_eq!(
        saga.handle(&succeeded(&sent[1], "late"), now())
            .unwrap()
            .outcome,
        SagaOutcome::Ignored
    );
    // Member 0's timer after its reply: ignored.
    assert_eq!(
        saga.handle(&timed_out(&member_timers[0]), now())
            .unwrap()
            .outcome,
        SagaOutcome::Ignored
    );
    let settled = saga.handle(&succeeded(&sent[2], "t"), now()).unwrap();
    let failure = saga.failure().unwrap();
    assert!(failure.timed_out);
    // The timed-out member may have applied: it is compensated too.
    assert_eq!(drain_compensations(&mut saga, settled.messages), [2, 1, 0]);
    assert_eq!(saga.status(), SagaStatus::Compensated);
    assert!(
        !saga.data().reserved.contains_key(&1),
        "late output not folded"
    );

    // A late reply after the saga moved on is ignored as well.
    assert_eq!(
        saga.handle(&succeeded(&sent[1], "later"), now())
            .unwrap()
            .outcome,
        SagaOutcome::Ignored,
        "a terminal saga ignores stragglers"
    );
}

#[test]
fn a_later_step_failure_compensates_every_fanned_out_member() {
    let mut saga = Saga::<StartMatch>::new();
    let started = saga.start("m4", data(3), now()).unwrap();
    let mut next = Vec::new();
    for command in commands(&started.messages) {
        next = saga
            .handle(&succeeded(command, "t"), now())
            .unwrap()
            .messages;
    }
    let confirm = commands(&next)[0].clone();
    let compensating = saga.handle(&failed(&confirm, "zone full"), now()).unwrap();
    assert_eq!(
        drain_compensations(&mut saga, compensating.messages),
        [2, 1, 0]
    );
    assert_eq!(saga.status(), SagaStatus::Compensated);
}

#[test]
fn member_compensation_retries_carry_the_member() {
    let mut saga = Saga::<StartMatch>::new();
    let started = saga.start("m5", data(2), now()).unwrap();
    let sent: Vec<OutboxMessage> = commands(&started.messages).into_iter().cloned().collect();
    saga.handle(&succeeded(&sent[0], "t"), now()).unwrap();
    let settled = saga.handle(&failed(&sent[1], "no"), now()).unwrap();
    let release = commands(&settled.messages)[0].clone();
    assert_eq!(release.id(), "test.match_start:m5:reserve:compensation:1:0");
    let retry = saga.handle(&failed(&release, "busy"), now()).unwrap();
    let again = commands(&retry.messages)[0].clone();
    assert_eq!(again.id(), "test.match_start:m5:reserve:compensation:2:0");
    assert_eq!(member(&again), 0);
    let cursor = saga.cursor().unwrap();
    assert_eq!(
        (cursor.phase, cursor.member, cursor.attempt),
        (SagaPhase::Compensation, Some(0), 2)
    );
    saga.handle(&succeeded(&again, "released"), now()).unwrap();
    assert_eq!(saga.status(), SagaStatus::Compensated);
}

#[test]
fn fan_out_is_bounded() {
    let mut empty = Saga::<StartMatch>::new();
    assert!(matches!(
        empty.start("m6", data(0), now()),
        Err(SagaError::Definition(_))
    ));
    let mut huge = Saga::<StartMatch>::new();
    assert!(matches!(
        huge.start("m7", data(MAX_SAGA_FAN_OUT + 1), now()),
        Err(SagaError::Definition(_))
    ));
    let mut max = Saga::<StartMatch>::new();
    let started = max.start("m8", data(MAX_SAGA_FAN_OUT), now()).unwrap();
    assert_eq!(commands(&started.messages).len(), MAX_SAGA_FAN_OUT);
}

#[test]
fn malformed_member_replies_are_rejected() {
    let mut saga = Saga::<StartMatch>::new();
    let started = saga.start("m9", data(2), now()).unwrap();
    let command = commands(&started.messages)[0].clone();
    let mut reply = succeeded(&command, "t");
    reply.member = None;
    assert!(matches!(
        saga.handle(&reply, now()),
        Err(SagaError::Malformed(_))
    ));
    reply.member = Some(7);
    assert!(matches!(
        saga.handle(&reply, now()),
        Err(SagaError::Malformed(_))
    ));
}

/// Every transition committed through a repository and reloaded from the
/// saga stream reproduces the live state exactly.
#[tokio::test]
async fn replaying_the_saga_stream_reproduces_fan_out_state() {
    let clock = ManualClock::new(now());
    let runner = SagaRunner::<_, StartMatch, _>::with_clock(InMemoryRepository::new(), clock);
    let mut live = Saga::<StartMatch>::new();

    let started = runner.start("m10", data(4)).await.unwrap();
    live.start("m10", data(4), now()).unwrap();
    let sent: Vec<OutboxMessage> = commands(&started.messages).into_iter().cloned().collect();
    let member_timers: Vec<OutboxMessage> =
        timers(&started.messages).into_iter().cloned().collect();
    let mut inbound = vec![
        succeeded(&sent[0], "a"),
        failed(&sent[1], "left"),
        timed_out(&member_timers[2]),
        succeeded(&sent[2], "late"),
        succeeded(&sent[3], "d"),
    ];
    let mut checkpoints = 0;
    while !inbound.is_empty() {
        let message = inbound.remove(0);
        let committed = runner.handle(&message).await.unwrap();
        let expected = live.handle(&message, now()).unwrap();
        assert_eq!(committed.outcome, expected.outcome);
        for command in commands(&committed.messages) {
            inbound.push(succeeded(command, "released"));
        }
        let replayed = runner.get("m10").await.unwrap().unwrap();
        assert_eq!(format!("{replayed:?}"), format!("{live:?}"));
        assert_eq!(replayed.data(), live.data());
        assert_eq!(replayed.fan_out(), live.fan_out());
        assert_eq!(replayed.cursor(), live.cursor());
        assert!(
            replayed.has_processed(&message.message_id)
                || committed.outcome != SagaOutcome::Applied
        );
        checkpoints += 1;
    }
    let replayed = runner.get("m10").await.unwrap().unwrap();
    assert_eq!(replayed.status(), SagaStatus::Compensated);
    assert_eq!(
        replayed.data().reserved.keys().copied().collect::<Vec<_>>(),
        [0, 3]
    );
    assert!(checkpoints >= 8);
}
