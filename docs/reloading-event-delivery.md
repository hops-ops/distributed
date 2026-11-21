# Event delivery during application reload

A supervisor generation gate is temporary infrastructure state, not a domain
rejection. The gate is an explicit value: the process resolves
`LifecycleGate::from_env()` once where it composes its services and passes it
to each with `Service::with_lifecycle`; a service given no gate is always open.
A GraphQL engine built from the service (`.service(&service)`) takes the same
gate. The gate answers from memory and follows the supervisor's membership file
in the background every 50 ms, so it lags the supervisor by at most that long;
the supervisor waits at least 500 ms after fencing before replacing a process.

Both direct command admission and bus dispatch return typed
`ApplicationReloading` while closed. Bus conversion classifies it as retryable
and retains/stops the receive loop: NAK the exact delivery, then surface the
error to the host's bounded restart policy. It must never Ack, Term or apply
ordinary permanent-failure policy. After activation the same delivery can run.

Business validation/authorization failures retain their existing permanent
classification and configured settlement policy. No gate bypass or implicit
success is introduced. The NAK uses the transport's ordinary retry spacing
(NATS JetStream: delivery-count backoff, see
[consumer delivery lanes and retry backoff](consumer-delivery-lanes.md)); the
gate adds no delay of its own.
