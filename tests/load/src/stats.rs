//! Latency percentile helpers and the JSON run report.

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cell: Option<String>,
    pub scenario: String,
    pub url: String,
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bus: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_frequency: Option<u64>,
    pub concurrency: usize,
    pub duration_secs: f64,
    pub warmup_secs: f64,
    pub ok: u64,
    pub err: u64,
    pub throughput_rps: f64,
    pub latency_ms: LatencySummary,
}

#[derive(Clone, Debug, Serialize)]
pub struct LatencySummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

/// `samples` is a sorted list of latencies in milliseconds.
pub fn percentile_ms(sorted_ms: &[f64], pct: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let rank = ((pct / 100.0) * (sorted_ms.len() as f64 - 1.0)).round() as usize;
    round3(sorted_ms[rank.min(sorted_ms.len() - 1)])
}

pub fn summarize(samples_ms: &mut [f64]) -> LatencySummary {
    samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    LatencySummary {
        p50: percentile_ms(samples_ms, 50.0),
        p95: percentile_ms(samples_ms, 95.0),
        p99: percentile_ms(samples_ms, 99.0),
        max: round3(samples_ms.last().copied().unwrap_or(0.0)),
    }
}

#[cfg(test)]
mod tests {
    use super::percentile_ms;

    #[test]
    fn percentile_picks_nearest_rank() {
        let samples = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile_ms(&samples, 0.0), 1.0);
        assert_eq!(percentile_ms(&samples, 50.0), 3.0);
        assert_eq!(percentile_ms(&samples, 100.0), 5.0);
        assert_eq!(percentile_ms(&[], 50.0), 0.0);
    }
}
