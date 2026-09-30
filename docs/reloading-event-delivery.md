# Event delivery during application reload

A supervisor generation gate is temporary infrastructure state, not a domain
rejection. Both direct command admission and bus dispatch return typed
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
