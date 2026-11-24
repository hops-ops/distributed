use super::*;

pub(crate) const MAX_LIVE_RESUME_PROJECTION_BYTES: usize = 512;

/// Parse the private request extension used by generated live operations.
/// Invalid input is a conservative reset signal, never trusted cursor state.
pub(crate) fn parse_requested_live_resume(request: &Request) -> RequestedLiveResume {
    let Some(distributed) = request.extensions.get("distributed") else {
        return RequestedLiveResume::Absent;
    };
    let Ok(distributed) = distributed.clone().into_json() else {
        return RequestedLiveResume::Invalid;
    };
    let Some(distributed) = distributed.as_object() else {
        return RequestedLiveResume::Invalid;
    };
    let Some(resume) = distributed.get("resume") else {
        return RequestedLiveResume::Absent;
    };
    let Some(cursors) = resume
        .as_object()
        .and_then(|resume| resume.get("cursors"))
        .and_then(serde_json::Value::as_array)
    else {
        return RequestedLiveResume::Invalid;
    };
    if cursors.len() > MAX_LIVE_RESUME_CURSORS {
        return RequestedLiveResume::Invalid;
    }

    let mut parsed = Vec::with_capacity(cursors.len());
    for cursor in cursors {
        let Some(cursor) = cursor.as_object() else {
            return RequestedLiveResume::Invalid;
        };
        let Some(projection) = cursor.get("projection").and_then(serde_json::Value::as_str) else {
            return RequestedLiveResume::Invalid;
        };
        if projection.is_empty() || projection.len() > MAX_LIVE_RESUME_PROJECTION_BYTES {
            return RequestedLiveResume::Invalid;
        }
        let Some(position) = cursor.get("position").and_then(serde_json::Value::as_str) else {
            return RequestedLiveResume::Invalid;
        };
        let Ok(parsed_position) = position.parse::<u64>() else {
            return RequestedLiveResume::Invalid;
        };
        if parsed_position.to_string() != position {
            return RequestedLiveResume::Invalid;
        }
        let Some(token) = cursor.get("token").and_then(serde_json::Value::as_str) else {
            return RequestedLiveResume::Invalid;
        };
        let Ok(token) = OpaqueProtocolToken::parse(token) else {
            return RequestedLiveResume::Invalid;
        };
        parsed.push(DistributedLiveCursor {
            projection: projection.to_string(),
            position: position.to_string(),
            token,
        });
    }
    RequestedLiveResume::Cursors(parsed)
}

#[cfg(test)]
mod live_resume_request_tests {
    use super::*;

    fn request_with_resume(value: serde_json::Value) -> Request {
        serde_json::from_value(serde_json::json!({
            "query": "subscription Watch { todos { id } }",
            "extensions": { "distributed": { "resume": value } }
        }))
        .expect("GraphQL request")
    }

    #[test]
    fn live_resume_request_is_bounded_and_canonical() {
        let token = ProtocolTokenCodec::new([9; 32])
            .issue(ProtocolTokenPurpose::LiveResume, &("bounded-test", 7_u64))
            .unwrap();
        let request = request_with_resume(serde_json::json!({
            "cursors": [{
                "projection": "todos",
                "position": "7",
                "token": token.as_str()
            }]
        }));
        let RequestedLiveResume::Cursors(cursors) = parse_requested_live_resume(&request) else {
            panic!("valid cursor must parse")
        };
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].projection, "todos");
        assert_eq!(cursors[0].position, "7");

        for invalid in [
            serde_json::json!({"cursors": [{
                "projection": "todos", "position": "07", "token": token.as_str()
            }]}),
            serde_json::json!({"cursors": [{
                "projection": "todos", "position": "7", "token": "not-a-token"
            }]}),
            serde_json::json!({"cursors": "not-an-array"}),
        ] {
            assert_eq!(
                parse_requested_live_resume(&request_with_resume(invalid)),
                RequestedLiveResume::Invalid
            );
        }

        let too_many = vec![
            serde_json::json!({
                "projection": "todos",
                "position": "7",
                "token": token.as_str()
            });
            MAX_LIVE_RESUME_CURSORS + 1
        ];
        assert_eq!(
            parse_requested_live_resume(&request_with_resume(
                serde_json::json!({"cursors": too_many})
            )),
            RequestedLiveResume::Invalid
        );
    }

    #[test]
    fn request_without_resume_remains_a_fresh_subscription() {
        let request: Request = serde_json::from_value(serde_json::json!({
            "query": "subscription Watch { todos { id } }",
            "extensions": { "distributed": {} }
        }))
        .unwrap();
        assert_eq!(
            parse_requested_live_resume(&request),
            RequestedLiveResume::Absent
        );
    }
}
