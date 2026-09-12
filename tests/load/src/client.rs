//! Load driver over any [`Invoker`].

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::invoke::{command_body, setup_hot_id, BusInvoker, Invoker};
use crate::stats::{summarize, RunReport};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    UniqueCreate,
    HotIncrement,
}

impl Scenario {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "unique-create" | "create" => Ok(Self::UniqueCreate),
            "hot-increment" | "increment" => Ok(Self::HotIncrement),
            other => Err(format!(
                "unknown --scenario {other:?} (expected unique-create or hot-increment)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::UniqueCreate => "unique-create",
            Self::HotIncrement => "hot-increment",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub url: String,
    pub scenario: Scenario,
    pub concurrency: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub repo: Option<String>,
    pub dispatch: Option<String>,
    pub bus: Option<String>,
    pub lock: Option<String>,
    pub snapshot_frequency: Option<u64>,
    pub cell: Option<String>,
    pub pipelined: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8790".into(),
            scenario: Scenario::UniqueCreate,
            concurrency: 32,
            duration: Duration::from_secs(15),
            warmup: Duration::from_secs(2),
            repo: None,
            dispatch: None,
            bus: None,
            lock: None,
            snapshot_frequency: None,
            cell: None,
            pipelined: false,
        }
    }
}

pub async fn run_client(
    config: ClientConfig,
) -> Result<RunReport, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(config.concurrency.max(1))
        .build()?;
    let invoker = Invoker::Http {
        client,
        base: config.url.trim_end_matches('/').to_string(),
    };
    run_invoker(invoker, config).await
}

pub async fn run_invoker(
    invoker: Invoker,
    config: ClientConfig,
) -> Result<RunReport, Box<dyn std::error::Error + Send + Sync>> {
    if config.concurrency == 0 {
        return Err("concurrency must be >= 1".into());
    }
    let hot_id = if config.scenario == Scenario::HotIncrement {
        Some(setup_hot_id(&invoker).await?)
    } else {
        None
    };

    let measuring = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(false));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(Mutex::new(Vec::<f64>::new()));

    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let invoker = invoker.clone();
        let scenario = config.scenario;
        let hot_id = hot_id.clone();
        let measuring = Arc::clone(&measuring);
        let paused = Arc::clone(&paused);
        let in_flight = Arc::clone(&in_flight);
        let stop = Arc::clone(&stop);
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);
        let samples = Arc::clone(&samples);
        workers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if paused.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                    continue;
                }
                in_flight.fetch_add(1, Ordering::SeqCst);
                if paused.load(Ordering::SeqCst) {
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
                let (command, body) = command_body(scenario, hot_id.as_deref());
                let started = Instant::now();
                let result = invoker.invoke(&command, body).await;
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                if !measuring.load(Ordering::Relaxed) {
                    continue;
                }
                match result {
                    Ok(()) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        samples.lock().await.push(elapsed_ms);
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    tokio::time::sleep(config.warmup).await;
    let pipelined_baseline = if config.pipelined {
        if let Invoker::Bus(bus) = &invoker {
            paused.store(true, Ordering::SeqCst);
            while in_flight.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
            drain_pipeline(bus, Duration::from_secs(8)).await;
            Some((
                bus.applied_ok.load(Ordering::Relaxed),
                bus.applied_err.load(Ordering::Relaxed),
            ))
        } else {
            None
        }
    } else {
        None
    };
    measuring.store(true, Ordering::Relaxed);
    paused.store(false, Ordering::SeqCst);
    let started = Instant::now();
    tokio::time::sleep(config.duration).await;
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        let _ = worker.await;
    }

    let (ok, err) = if let Some((baseline_ok, baseline_err)) = pipelined_baseline {
        if let Invoker::Bus(bus) = &invoker {
            drain_pipeline(bus, Duration::from_secs(8)).await;
            (
                bus.applied_ok
                    .load(Ordering::Relaxed)
                    .saturating_sub(baseline_ok),
                bus.applied_err
                    .load(Ordering::Relaxed)
                    .saturating_sub(baseline_err),
            )
        } else {
            (ok.load(Ordering::Relaxed), err.load(Ordering::Relaxed))
        }
    } else {
        (ok.load(Ordering::Relaxed), err.load(Ordering::Relaxed))
    };
    let mut samples = samples.lock().await;
    let latency_ms = summarize(&mut samples);
    let secs = elapsed.as_secs_f64().max(0.000_001);
    Ok(RunReport {
        cell: config.cell,
        scenario: config.scenario.as_str().into(),
        url: config.url,
        repo: config.repo,
        dispatch: config.dispatch,
        bus: config.bus,
        lock: config.lock,
        snapshot_frequency: config.snapshot_frequency,
        concurrency: config.concurrency,
        duration_secs: (secs * 1000.0).round() / 1000.0,
        warmup_secs: config.warmup.as_secs_f64(),
        ok,
        err,
        throughput_rps: (ok as f64 / secs * 10.0).round() / 10.0,
        latency_ms,
    })
}

/// Wait until applied counts stop moving, so pipelined enqueue is not
/// under-counted just because the measure window ended.
async fn drain_pipeline(bus: &BusInvoker, budget: Duration) {
    let deadline = Instant::now() + budget;
    let mut last = bus.applied_ok.load(Ordering::Relaxed) + bus.applied_err.load(Ordering::Relaxed);
    let mut last_change = Instant::now();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = bus.applied_ok.load(Ordering::Relaxed) + bus.applied_err.load(Ordering::Relaxed);
        if now != last {
            last = now;
            last_change = Instant::now();
        } else if last_change.elapsed() >= Duration::from_secs(1) {
            break;
        }
    }
}
