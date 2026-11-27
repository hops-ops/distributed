//! Queue consumer boundary: celld provides durable push/retry, while the
//! native Distributed bus provides command routing and event fanout.

use distributed::bus::{CelldQueueEnvelope, CelldQueueHttpPublisher, CelldQueueRelay};
use distributed::microsvc::cell_host::CELL_INTERNAL_SECRET_HEADER;
use worker::{event, Context, Env, Error, MessageBatch, MessageExt, Result};

#[event(queue)]
pub async fn main(batch: MessageBatch<CelldQueueEnvelope>, env: Env, _ctx: Context) -> Result<()> {
    console_error_panic_hook::set_once();
    let endpoint = required_var(&env, "CELLD_QUEUE_RELAY_URL")?;
    let internal_secret = required_var(&env, "DISTRIBUTED_INTERNAL_SECRET")?;
    let publisher = if local_test_mode(&env)? {
        CelldQueueHttpPublisher::new_local_test(endpoint, &internal_secret)
    } else {
        CelldQueueHttpPublisher::new(endpoint)
    }
    .map_err(|error| Error::RustError(error.to_string()))?
    .with_header(CELL_INTERNAL_SECRET_HEADER, internal_secret);
    let relay = CelldQueueRelay::new(publisher);

    for delivery in batch.messages()? {
        match relay.relay(delivery.body().clone()).await {
            Ok(()) => delivery.ack(),
            Err(error) => {
                worker::console_error!("celld Queue relay failed for {}: {}", delivery.id(), error);
                delivery.retry();
            }
        }
    }
    Ok(())
}

fn local_test_mode(env: &Env) -> Result<bool> {
    match env.var("CELLD_QUEUE_RELAY_LOCAL_TEST") {
        Err(_) => Ok(false),
        Ok(value) if value.to_string() == "1" => Ok(true),
        Ok(_) => Err(Error::RustError(
            "CELLD_QUEUE_RELAY_LOCAL_TEST must be exactly `1` when enabled".into(),
        )),
    }
}

fn required_var(env: &Env, name: &str) -> Result<String> {
    env.var(name)
        .map(|value| value.to_string())
        .map_err(|_| Error::RustError(format!("{name} is required")))
}
