//! Message transports: in-memory, SQLite, Postgres, NATS, RabbitMQ, Kafka, Redis Streams and Knative.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test transports <name>::` runs what `--test <name>` ran.

#[cfg(feature = "kafka")]
mod kafka_transport;
#[cfg(feature = "knative")]
mod knative_cloudevents;
#[cfg(feature = "nats")]
mod nats_transport;
#[cfg(feature = "postgres")]
mod postgres_transport;
#[cfg(feature = "rabbitmq")]
mod rabbitmq_transport;
#[cfg(feature = "redis-streams")]
mod redis_streams;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod sql_bus_conformance;
#[cfg(feature = "sqlite")]
mod sqlite_transport;
mod transport_in_memory;
mod transport_typed_commands;
