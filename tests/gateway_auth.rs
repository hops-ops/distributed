#![cfg(feature = "gateway")]
mod gateway_support;
use distributed::gateway::*;
use gateway_support::run;

struct Provider {
    revision: &'static str,
}
impl AuthProvider for Provider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<RequestContext, AuthError> {
        let (identity, backend) = match credentials.authorization.as_deref() {
            None => (None, BackendCredential::None),
            Some("valid") | Some("expired") => (
                Some(
                    Identity::verified(
                        "issuer",
                        "alice",
                        vec!["user".into()],
                        if credentials.authorization.as_deref() == Some("expired") {
                            10
                        } else {
                            100
                        },
                    )
                    .unwrap(),
                ),
                BackendCredential::Bearer("validated-token".into()),
            ),
            _ => return Err(AuthError::Unauthorized),
        };
        Ok(RequestContext::from_provider(identity, self.revision, backend).unwrap())
    }
}

#[test]
fn shared_context_and_delegated_handlers() {
    let provider = Provider { revision: "v1" };
    let anonymous = run(provider.authenticate(&Credentials::default())).unwrap();
    assert_eq!(Admission::Public.check(&anonymous, 20), Ok(()));
    assert_eq!(
        Admission::Authenticated.check(&anonymous, 20),
        Err(AuthError::Unauthorized)
    );
    let credentials = Credentials {
        authorization: Some("valid".into()),
        cookie: None,
    };
    let context = run(provider.authenticate(&credentials)).unwrap();
    for _route in ["UI", "API", "custom", "protected-assets"] {
        assert_eq!(Admission::Authenticated.check(&context, 20), Ok(()));
        assert_eq!(context.identity().unwrap().subject(), "alice");
    }
    // Route admission is not backend authorization; a more privileged action
    // still fails its own policy after successful gateway admission.
    assert_eq!(
        Admission::Role("admin".into()).check(&context, 20),
        Err(AuthError::Forbidden)
    );
    let replacement = run(Provider { revision: "v2" }.authenticate(&credentials)).unwrap();
    assert_ne!(context.provider_revision(), replacement.provider_revision());
    assert!(run(provider.authenticate(&Credentials {
        authorization: Some("invalid".into()),
        cookie: None
    }))
    .is_err());
    let expired = run(provider.authenticate(&Credentials {
        authorization: Some("expired".into()),
        cookie: None,
    }))
    .unwrap();
    assert_eq!(
        Admission::Public.check(&expired, 20),
        Err(AuthError::Unauthorized)
    );
    assert_eq!(
        Admission::Authenticated.check(&context, 100),
        Err(AuthError::Unauthorized)
    );
}

#[test]
fn credentials_and_assertions_cannot_leak_or_be_guessed() {
    let credentials = Credentials {
        authorization: Some("secret-token".into()),
        cookie: Some("session-secret".into()),
    };
    let debug = format!(
        "{credentials:?} {:?}",
        BackendCredential::Bearer("secret-token".into())
    );
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("session-secret"));
    assert!(
        RequestContext::from_provider(None, "v1", BackendCredential::Bearer("token".into()))
            .is_err()
    );
    for name in [
        "X-User-Id",
        "X-Roles",
        "X-Hasura-Allowed-Roles",
        "Forwarded",
        "X-Forwarded-Host",
        "CF-Access-Jwt-Assertion",
    ] {
        assert!(is_untrusted_identity_header(name));
    }
    assert!(!is_untrusted_identity_header("content-type"));
    // A bearer-only provider cannot promote a session cookie into a token.
    let context = run(Provider { revision: "v1" }.authenticate(&Credentials {
        authorization: None,
        cookie: Some("valid".into()),
    }))
    .unwrap();
    assert!(context.identity().is_none());
}
