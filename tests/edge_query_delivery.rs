#![cfg(feature = "gateway-delivery")]
use distributed::gateway::delivery::*;
use serde_json::json;
fn identity(subject: &str) -> OriginIdentity {
    OriginIdentity {
        application: "todo".into(),
        endpoint: "api".into(),
        schema_hash: "schema-1".into(),
        protocol_hash: "protocol-1".into(),
        authorization_generation: "policy-1".into(),
        cache_scope: subject.into(),
    }
}
fn context() -> FreshnessContext {
    FreshnessContext::parse(&json!({"version":1,"schemaHash":"schema-1","protocolHash":"protocol-1","authorizationGeneration":"policy-1","cacheScope":"alice","pending":[],"minimum":[]})).unwrap()
}
fn models(names: &[&str]) -> Dependencies {
    Dependencies {
        complete: true,
        models: names.iter().map(|v| v.to_string()).collect(),
        relationships: Default::default(),
    }
}
fn index(scope: &str, position: &str) -> Minimum {
    Minimum::Index {
        projection: "todos".into(),
        scope_token: scope.into(),
        position: position.into(),
    }
}
#[test]
fn origin_identity_canonical_variables_and_selected_operation() {
    let alice = identity("alice");
    let bob = identity("bob");
    let a = json!({"query":"query A($filter: Input) { todos(filter: $filter) { title } } query B { count }", "operationName":"A", "variables":{"filter":{"a":1,"b":[2,3]}}});
    let mut b = a.clone();
    b["variables"] = json!({"filter":{"b":[2,3],"a":1}});
    assert_eq!(
        OperationKey::from_origin(&alice, &a),
        OperationKey::from_origin(&alice, &b)
    );
    assert_ne!(
        OperationKey::from_origin(&alice, &a),
        OperationKey::from_origin(&bob, &a)
    );
    b["operationName"] = "B".into();
    assert_ne!(
        OperationKey::from_origin(&alice, &a),
        OperationKey::from_origin(&alice, &b)
    );
    b["operationName"] = "Absent".into();
    assert!(OperationKey::from_origin(&alice, &b).is_err());
    for document in [
        "mutation { edit }",
        "query { commandStatus(commandId: \"one\") { state } }",
        "query { todos { id } commandStatus(commandId: \"one\") { state } }",
    ] {
        assert!(OperationKey::from_origin(&alice, &json!({"query":document})).is_err());
    }
    let mut changed = alice.clone();
    changed.authorization_generation = "policy-2".into();
    assert_ne!(
        OperationKey::from_origin(&alice, &a),
        OperationKey::from_origin(&changed, &a)
    );
    let mut order = a.clone();
    order["variables"]["filter"]["b"] = json!([3, 2]);
    assert_ne!(
        OperationKey::from_origin(&alice, &a),
        OperationKey::from_origin(&alice, &order)
    );
}
#[test]
fn eventual_pending_and_confirmed_minimum() {
    let mut request = context();
    request.pending.push(models(&["Todo"]));
    for dependency in [
        models(&["Todo"]),
        models(&["Count", "Todo"]),
        models(&["Todo", "Owner"]),
        Dependencies::default(),
    ] {
        assert_eq!(
            read_target(ReadConsistency::StaleTolerant, &dependency, Some(&request)),
            ReadTarget::Primary
        );
    }
    assert_eq!(
        read_target(
            ReadConsistency::StaleTolerant,
            &models(&["Blob"]),
            Some(&request)
        ),
        ReadTarget::Replica
    );
    request.observe([index("scope-1", "2")]).unwrap();
    request.pending.clear();
    assert!(!request.satisfied_by(&[index("scope-1", "1")]));
    assert!(request.satisfied_by(&[index("scope-1", "2")]));
    assert_eq!(
        read_target(
            ReadConsistency::StaleTolerant,
            &models(&["Todo"]),
            Some(&request)
        ),
        ReadTarget::Primary
    );
    request
        .observe([index("scope-1", "3"), index("incomparable", "1")])
        .unwrap();
    assert_eq!(request.minimum.len(), 2);
    assert!(!request.satisfied_by(&[index("scope-1", "999")]));
}
#[test]
fn atomic_minimum_survives_confirmation() {
    let mut request = context();
    let record = |incarnation: &str, revision: &str| Minimum::Record {
        model: "Blob".into(),
        scope_token: "record-1".into(),
        incarnation: incarnation.into(),
        revision: revision.into(),
    };
    request.observe([record("1", "9")]).unwrap();
    assert!(request.pending.is_empty());
    assert!(!request.satisfied_by(&[record("1", "8")]));
    request.observe([record("2", "1")]).unwrap();
    assert_eq!(request.minimum, [record("2", "1")]);
    assert!(!request.satisfied_by(&[record("1", "999")]));
    assert!(request.satisfied_by(&[record("2", "1")]));
    assert_eq!(
        request.bind(&identity("bob")),
        Err(DeliveryError::ScopeChanged)
    );
}
#[test]
fn invalid_context_never_weakens_routing() {
    let value = serde_json::to_value(context()).unwrap();
    for field in ["version", "cacheScope", "minimum", "pending"] {
        let mut forged = value.clone();
        forged[field] = json!(-1);
        assert!(FreshnessContext::parse(&forged).is_err());
    }
    let mut large = value.clone();
    large["minimum"] = json!(vec![
        serde_json::to_value(index("scope", "1")).unwrap();
        257
    ]);
    assert!(FreshnessContext::parse(&large).is_err());
    assert!(index("scope", "18446744073709551616").validate().is_err());
    assert!(index("scope", "01").validate().is_err());
    assert_eq!(
        read_target(ReadConsistency::Current, &models(&[]), None),
        ReadTarget::Primary
    );
}

fn admission(validator: &str) -> OriginAdmission {
    let identity = identity("alice");
    OriginAdmission {
        key: OperationKey::from_origin(&identity, &json!({"query":"{ todos { title } }"})).unwrap(),
        identity,
        operation: "document-fingerprint".into(),
        validator: validator.into(),
        validated_at: 100,
        expires_at: 200,
        policy: SnapshotPolicy::Current,
    }
}
fn snapshot(admission: &OriginAdmission) -> SnapshotResponse {
    SnapshotResponse { status: 200, headers: vec![("content-type".into(), "application/json".into())], body: serde_json::to_vec(&json!({
        "data":{"todos":[]}, "extensions": {
            "gatewayDelivery":{"validator":admission.validator},
            "distributed": {"protocolVersion":1,"schemaHash":admission.identity.schema_hash,
            "authorizationGeneration":admission.identity.authorization_generation,"cacheScope":admission.identity.cache_scope,
            "operation":admission.operation,"snapshot":{"recordsComplete":true,"indexesComparable":true,"records":[],
                "indexes":[{"projection":"todos","scopeToken":"scope","position":"2"}]}}
        }
    })).unwrap() }
}
#[test]
fn private_validation_public_age_and_late_fill_fence() {
    let mut cache = SnapshotCache::new(SnapshotLimits::default()).unwrap();
    let first = admission("v1");
    let body = snapshot(&first);
    let ticket = cache.begin_fill(&first, 100).unwrap();
    assert!(cache
        .install(ticket, first.clone(), body.clone(), 100)
        .unwrap());
    assert_eq!(
        cache.lookup(&first, None, 101).unwrap().unwrap().body,
        body.body
    );
    let newer = admission("v2");
    assert!(cache.lookup(&newer, None, 101).unwrap().is_none());
    let ticket = cache.begin_fill(&first, 100).unwrap();
    assert!(
        !cache
            .install(ticket, newer.clone(), body.clone(), 101)
            .unwrap(),
        "old bytes cannot acquire new version"
    );
    let late = cache.begin_fill(&first, 100).unwrap();
    cache.invalidate_all();
    assert!(!cache
        .install(late, first.clone(), body.clone(), 101)
        .unwrap());
    assert!(cache.is_empty());
    let mut public = first.clone();
    public.policy = SnapshotPolicy::Public {
        max_age_seconds: 10,
    };
    let ticket = cache.begin_fill(&public, 100).unwrap();
    assert!(cache
        .install(ticket, public.clone(), body.clone(), 100)
        .unwrap());
    let mut current = public.clone();
    current.validator = "v2".into();
    current.validated_at = 108;
    assert!(cache.lookup(&current, None, 108).unwrap().is_some());
    current.validated_at = 111;
    assert!(
        cache.lookup(&current, None, 111).unwrap().is_none(),
        "fresh admission cannot renew old public age"
    );
    assert!(
        cache.lookup(&current, None, 200).is_err(),
        "expired consumer cannot reuse public entry"
    );
    current.policy = SnapshotPolicy::Current;
    assert!(cache.lookup(&current, None, 112).unwrap().is_none());
}
#[test]
fn cache_envelope_eligibility_freshness_and_capacity() {
    let admission = admission("v1");
    let valid = snapshot(&admission);
    let mut floor = context();
    floor.observe([index("scope", "3")]).unwrap();
    assert!(!valid.satisfies(&admission, Some(&floor)));
    let mut floor = context();
    floor.observe([index("scope", "2")]).unwrap();
    assert!(valid.satisfies(&admission, Some(&floor)));
    for (name, value) in [
        ("Set-Cookie", "session=secret"),
        ("Cache-Control", "private, no-store"),
        ("Vary", "*"),
    ] {
        let mut response = valid.clone();
        response.headers.push((name.into(), value.into()));
        assert!(!response.satisfies(&admission, None));
    }
    for path in ["errors", "partial", "command", "protocol", "null"] {
        let mut response = valid.clone();
        let mut value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        match path {
            "errors" => value["errors"] = json!([{"message":"denied"}]),
            "partial" => {
                value["extensions"]["distributed"]["snapshot"]["recordsComplete"] = false.into()
            }
            "command" => value["extensions"]["distributed"]["command"] = json!({}),
            "protocol" => value["extensions"]["distributed"]["protocolVersion"] = 2.into(),
            _ => value["data"] = serde_json::Value::Null,
        }
        response.body = serde_json::to_vec(&value).unwrap();
        assert!(!response.satisfies(&admission, None), "{path}");
    }
    let mut cache = SnapshotCache::new(SnapshotLimits {
        entries: 1,
        bytes: 4096,
        entry_bytes: 2048,
    })
    .unwrap();
    let ticket = cache.begin_fill(&admission, 100).unwrap();
    assert!(cache
        .install(ticket, admission.clone(), valid.clone(), 100)
        .unwrap());
    let mut other = admission.clone();
    other.identity.cache_scope = "bob".into();
    other.key = OperationKey::from_origin(&other.identity, &json!({"query":"{ todos { title } }"}))
        .unwrap();
    let ticket = cache.begin_fill(&other, 100).unwrap();
    assert!(cache
        .install(ticket, other.clone(), snapshot(&other), 100)
        .unwrap());
    assert_eq!(cache.len(), 1);
    assert!(cache.lookup(&admission, None, 100).unwrap().is_none());
    let mut oversized = snapshot(&other);
    oversized.body.resize(4096, b' ');
    let ticket = cache.begin_fill(&other, 100).unwrap();
    assert!(!cache.install(ticket, other, oversized, 100).unwrap());
}

#[test]
fn flight_admission_limits_freshness_and_generation_fences() {
    let request = json!({"query":"{ todos { title } }"});
    let admission = admission("v1");
    let key = FlightKey::admitted(&admission, &request, None, 100).unwrap();
    let mut stronger = context();
    stronger.observe([index("scope", "3")]).unwrap();
    assert_ne!(
        key,
        FlightKey::admitted(&admission, &request, Some(&stronger), 100).unwrap()
    );
    let mut later = admission.clone();
    later.validator = "v2".into();
    assert_ne!(
        key,
        FlightKey::admitted(&later, &request, None, 100).unwrap()
    );
    assert!(FlightKey::admitted(&admission, &request, None, 200).is_err());
    let mut forged = context();
    forged.cache_scope = "bob".into();
    assert!(FlightKey::admitted(&admission, &request, Some(&forged), 100).is_err());
    let mut registry = FlightRegistry::new(FlightLimits {
        groups: 1,
        consumers: 100,
        deadline_ms: 1000,
        ..Default::default()
    })
    .unwrap();
    let mut tickets = Vec::new();
    for index in 0..100 {
        let (ticket, owner) = registry.join(key.clone(), 0).unwrap();
        assert_eq!(owner, index == 0);
        tickets.push(ticket);
    }
    assert_eq!(registry.consumers(), 100);
    assert!(registry.join(key.clone(), 1).is_err());
    assert!(!registry.leave(tickets.pop().unwrap()));
    assert_eq!(registry.consumers(), 99);
    for ticket in tickets {
        registry.leave(ticket);
    }
    assert!(registry.is_empty());
    let (old, _) = registry.join(key.clone(), 100).unwrap();
    let (new, owner) = registry.join(key.clone(), 1100).unwrap();
    assert!(owner);
    assert!(
        !registry.leave(old),
        "expired owner cannot release new generation"
    );
    assert_eq!(registry.consumers(), 1);
    assert!(registry.leave(new));
    assert!(registry.is_empty());
}

#[test]
fn live_scope_replay_and_proof_sensitive_frames() {
    let request = json!({"query":"subscription { todos { title } }"});
    let mut admitted = admission("v1");
    admitted.key = OperationKey::from_origin(&admitted.identity, &request).unwrap();
    let key = LiveKey::admitted(&admitted, &request, None, 100).unwrap();
    let mut resumed = request.clone();
    resumed["extensions"] =
        json!({"distributed":{"resume":[{"projection":"todos","position":"1","token":"cursor"}]}});
    let mut replay = admitted.clone();
    replay.key = OperationKey::from_origin(&replay.identity, &resumed).unwrap();
    let replay_key = LiveKey::admitted(&replay, &resumed, None, 100).unwrap();
    assert!(key.same_operation(&replay_key));
    assert!(!key.same_initial(&replay_key));
    for changed in ["subject", "policy"] {
        let mut other = admitted.clone();
        if changed == "subject" {
            other.identity.cache_scope = "bob".into();
        } else {
            other.identity.authorization_generation = "policy-2".into();
        }
        other.key = OperationKey::from_origin(&other.identity, &request).unwrap();
        assert!(!key.same_operation(&LiveKey::admitted(&other, &request, None, 100).unwrap()));
    }
    assert!(LiveKey::admitted(&admitted, &request, None, 200).is_err());
    assert!(LiveKey::admitted(
        &admission("v1"),
        &json!({"query":"{ todos { title } }"}),
        None,
        100
    )
    .is_err());
    let mut payload: serde_json::Value = serde_json::from_slice(&snapshot(&admitted).body).unwrap();
    payload["extensions"]["distributed"]["live"] = json!({"supported":true,"cursors":[{"projection":"todos","position":"2","token":"cursor"}]});
    let first = LiveFrame::from_origin(&admitted, payload.clone(), None, 4096).unwrap();
    assert!(
        first.same_frame(&LiveFrame::from_origin(&admitted, payload.clone(), None, 4096).unwrap())
    );
    payload["extensions"]["distributed"]["observations"] = json!([{"commandId":"confirmed"}]);
    let proof = LiveFrame::from_origin(&admitted, payload.clone(), None, 4096).unwrap();
    assert!(
        !first.same_frame(&proof),
        "equal data with new evidence must be delivered"
    );
    assert!(first.same_cursor(&proof));
    payload["data"] = json!({"todos":[{"title":"external write"}]});
    let changed = LiveFrame::from_origin(&admitted, payload.clone(), None, 4096).unwrap();
    assert!(
        !first.same_cursor(&changed),
        "same projector cursor does not cover external writes"
    );
    payload["extensions"]["distributed"]["live"]["supported"] = false.into();
    let unsupported = LiveFrame::from_origin(&admitted, payload, None, 4096).unwrap();
    assert!(!unsupported.same_cursor(&unsupported));
    let mut stronger = context();
    stronger.observe([index("scope", "3")]).unwrap();
    assert!(!first.satisfies(&admitted, Some(&stronger)));
}
