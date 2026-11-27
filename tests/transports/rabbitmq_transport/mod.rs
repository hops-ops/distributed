//! RabbitMQ bus: the bus contract over `RabbitBus` against a broker. Skips
//! when `AMQP_URL` is unset.

use common::env as env_support;
use common::ids::unique;
use common::transport::bus::{BusCapabilities, BusHarness, KindCapabilities};
use distributed::bus::RabbitBus;

/// One namespace (exchange and queue prefix) per test; every bus is its own
/// connection.
struct Rabbit {
    url: String,
    namespace: String,
}

impl BusHarness for Rabbit {
    type Bus = RabbitBus;

    const CAPABILITIES: BusCapabilities = BusCapabilities {
        // A command goes to the queue named by its name, declared on send so
        // nothing sent before a listener starts is lost; a listener of two
        // names reads two queues.
        commands: KindCapabilities {
            dedupes_ids: false,
            retry_holds_position: false,
            ordered_across_names: false,
        },
        // A group's events share one queue bound to every name it handles.
        // A consumer holds its prefetched deliveries, so a requeued one comes
        // back behind them.
        events: KindCapabilities {
            dedupes_ids: false,
            retry_holds_position: false,
            ordered_across_names: true,
        },
        rebalance_may_redeliver: false,
        added_metadata: &[],
    };

    async fn start() -> Option<Self> {
        Some(Self {
            url: env_support::require_service("AMQP_URL", "rabbitmq transport test")?,
            namespace: unique("ns").to_lowercase(),
        })
    }

    async fn bus(&self) -> RabbitBus {
        RabbitBus::connect(&self.url)
            .namespace(&self.namespace)
            .await
            .expect("connect bus")
    }
}

common::bus_conformance_tests!(Rabbit);
