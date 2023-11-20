// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::time::{Duration, Instant};

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry};
use mz_ore::stats::HISTOGRAM_COUNT_BUCKETS;
use mz_postgres_client::metrics::PostgresClientMetrics;
use prometheus::{CounterVec, Histogram, HistogramVec, IntCounterVec};

use crate::coord::timestamp_oracle::retry::RetryStream;

/// Prometheus monitoring metrics for timestamp oracles.
///
/// Intentionally not Clone because we expect this to be passed around in an
/// Arc.
pub struct Metrics {
    _vecs: MetricsVecs,

    /// Metrics for
    /// [`TimestampOracle`](crate::coord::timestamp_oracle::TimestampOracle).
    pub oracle: OracleMetrics,

    /// Metrics recording how many operations we batch into one oracle call, for
    /// those operations that _do_ support batching, and only when using the
    /// `BatchingTimestampOracle` wrapper.
    pub batching: BatchingMetrics,

    /// Metrics for each retry loop.
    pub retries: RetriesMetrics,

    /// Metrics for [`PostgresClient`](mz_postgres_client::PostgresClient).
    pub postgres_client: PostgresClientMetrics,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Returns a new [Metrics] instance for the given `timeline`, connected to
    /// the given registry.
    pub fn new(registry: &MetricsRegistry, timeline: &str) -> Self {
        let vecs = MetricsVecs::new(registry);

        // It's a bit annoying that we're encoding the timeline in the metric's
        // name itself, but we don't want to change the labels on this one
        // because it's used outside the timestamp oracles and there are
        // existing dashboards for it.
        let pg_client_metrics_prefix = format!("mz_ts_oracle_{}", timeline);

        Metrics {
            oracle: vecs.oracle_metrics(timeline),
            batching: vecs.batching_metrics(timeline),
            retries: vecs.retries_metrics(timeline),
            postgres_client: PostgresClientMetrics::new(registry, &pg_client_metrics_prefix),
            _vecs: vecs,
        }
    }
}

#[derive(Debug)]
struct MetricsVecs {
    external_op_started: IntCounterVec,
    external_op_succeeded: IntCounterVec,
    external_op_failed: IntCounterVec,
    external_op_seconds: CounterVec,

    retry_started: IntCounterVec,
    retry_finished: IntCounterVec,
    retry_retries: IntCounterVec,
    retry_sleep_seconds: CounterVec,

    batched_op_count: HistogramVec,
}

impl MetricsVecs {
    fn new(registry: &MetricsRegistry) -> Self {
        MetricsVecs {
            external_op_started: registry.register(metric!(
                name: "mz_ts_oracle_started_count",
                help: "count of oracle operations started",
                var_labels: ["timeline", "op"],
            )),
            external_op_succeeded: registry.register(metric!(
                name: "mz_ts_oracle_succeeded_count",
                help: "count of oracle operations succeeded",
                var_labels: ["timeline", "op"],
            )),
            external_op_failed: registry.register(metric!(
                name: "mz_ts_oracle_failed_count",
                help: "count of oracle operations failed",
                var_labels: ["timeline", "op"],
            )),
            external_op_seconds: registry.register(metric!(
                name: "mz_ts_oracle_seconds",
                help: "time spent in oracle operations",
                var_labels: ["timeline", "op"],
            )),

            retry_started: registry.register(metric!(
                name: "mz_ts_oracle_retry_started_count",
                help: "count of retry loops started",
                var_labels: ["timeline", "op"],
            )),
            retry_finished: registry.register(metric!(
                name: "mz_ts_oracle_retry_finished_count",
                help: "count of retry loops finished",
                var_labels: ["timeline", "op"],
            )),
            retry_retries: registry.register(metric!(
                name: "mz_ts_oracle_retry_retries_count",
                help: "count of total attempts by retry loops",
                var_labels: ["timeline", "op"],
            )),
            retry_sleep_seconds: registry.register(metric!(
                name: "mz_ts_oracle_retry_sleep_seconds",
                help: "time spent in retry loop backoff",
                var_labels: ["timeline", "op"],
            )),

            batched_op_count: registry.register(metric!(
                name: "mz_ts_oracle_batched_op_count",
                help: "number of operations that were batched into one external operation",
                var_labels: ["timeline", "op"],
                buckets: HISTOGRAM_COUNT_BUCKETS.to_vec(),
            )),
        }
    }

    fn oracle_metrics(&self, timeline: &str) -> OracleMetrics {
        OracleMetrics {
            write_ts: self.external_op_metrics("write_ts", timeline),
            peek_write_ts: self.external_op_metrics("peek_write_ts", timeline),
            read_ts: self.external_op_metrics("read_ts", timeline),
            apply_write: self.external_op_metrics("apply_write", timeline),
        }
    }

    fn external_op_metrics(&self, op: &str, timeline: &str) -> ExternalOpMetrics {
        ExternalOpMetrics {
            started: self.external_op_started.with_label_values(&[timeline, op]),
            succeeded: self
                .external_op_succeeded
                .with_label_values(&[timeline, op]),
            failed: self.external_op_failed.with_label_values(&[timeline, op]),
            seconds: self.external_op_seconds.with_label_values(&[timeline, op]),
        }
    }

    fn batching_metrics(&self, timeline: &str) -> BatchingMetrics {
        BatchingMetrics {
            read_ts: self
                .batched_op_count
                .with_label_values(&[timeline, "read_ts"]),
        }
    }

    fn retries_metrics(&self, timeline: &str) -> RetriesMetrics {
        RetriesMetrics {
            open: self.retry_metrics("open", timeline),
            get_all_timelines: self.retry_metrics("get_all_timelines", timeline),
            write_ts: self.retry_metrics("write_ts", timeline),
            peek_write_ts: self.retry_metrics("peek_write_ts", timeline),
            read_ts: self.retry_metrics("read_ts", timeline),
            apply_write: self.retry_metrics("apply_write", timeline),
        }
    }

    fn retry_metrics(&self, name: &str, timeline: &str) -> RetryMetrics {
        RetryMetrics {
            name: name.to_owned(),
            started: self.retry_started.with_label_values(&[timeline, name]),
            finished: self.retry_finished.with_label_values(&[timeline, name]),
            retries: self.retry_retries.with_label_values(&[timeline, name]),
            sleep_seconds: self
                .retry_sleep_seconds
                .with_label_values(&[timeline, name]),
        }
    }
}

#[derive(Debug)]
pub struct ExternalOpMetrics {
    started: IntCounter,
    succeeded: IntCounter,
    failed: IntCounter,
    seconds: Counter,
}

impl ExternalOpMetrics {
    pub(crate) async fn run_op<R, F, OpFn>(&self, op_fn: OpFn) -> Result<R, anyhow::Error>
    where
        F: std::future::Future<Output = Result<R, anyhow::Error>>,
        OpFn: FnOnce() -> F,
    {
        self.started.inc();
        let start = Instant::now();
        let res = op_fn().await;
        let elapsed_seconds = start.elapsed().as_secs_f64();
        self.seconds.inc_by(elapsed_seconds);
        match res.as_ref() {
            Ok(_) => self.succeeded.inc(),
            Err(_err) => {
                self.failed.inc();
            }
        };
        res
    }
}

#[derive(Debug)]
pub struct OracleMetrics {
    pub write_ts: ExternalOpMetrics,
    pub peek_write_ts: ExternalOpMetrics,
    pub read_ts: ExternalOpMetrics,
    pub apply_write: ExternalOpMetrics,
}

#[derive(Debug)]
pub struct BatchingMetrics {
    pub read_ts: Histogram,
}

#[derive(Debug)]
pub struct RetryMetrics {
    pub(crate) name: String,
    pub(crate) started: IntCounter,
    pub(crate) finished: IntCounter,
    pub(crate) retries: IntCounter,
    pub(crate) sleep_seconds: Counter,
}

impl RetryMetrics {
    pub(crate) fn stream(&self, retry: RetryStream) -> MetricsRetryStream {
        MetricsRetryStream::new(retry, self)
    }
}

#[derive(Debug)]
pub struct RetriesMetrics {
    pub(crate) open: RetryMetrics,
    pub(crate) get_all_timelines: RetryMetrics,
    pub(crate) write_ts: RetryMetrics,
    pub(crate) peek_write_ts: RetryMetrics,
    pub(crate) read_ts: RetryMetrics,
    pub(crate) apply_write: RetryMetrics,
}

struct IncOnDrop(IntCounter);

impl Drop for IncOnDrop {
    fn drop(&mut self) {
        self.0.inc()
    }
}

pub struct MetricsRetryStream {
    retry: RetryStream,
    pub(crate) retries: IntCounter,
    sleep_seconds: Counter,
    _finished: IncOnDrop,
}

impl MetricsRetryStream {
    pub fn new(retry: RetryStream, metrics: &RetryMetrics) -> Self {
        metrics.started.inc();
        MetricsRetryStream {
            retry,
            retries: metrics.retries.clone(),
            sleep_seconds: metrics.sleep_seconds.clone(),
            _finished: IncOnDrop(metrics.finished.clone()),
        }
    }

    /// How many times [Self::sleep] has been called.
    pub fn attempt(&self) -> usize {
        self.retry.attempt()
    }

    /// The next sleep (without jitter for easy printing in logs).
    pub fn next_sleep(&self) -> Duration {
        self.retry.next_sleep()
    }

    /// Executes the next sleep in the series.
    ///
    /// This isn't cancel-safe, so it consumes and returns self, to prevent
    /// accidental mis-use.
    pub async fn sleep(self) -> Self {
        self.retries.inc();
        self.sleep_seconds
            .inc_by(self.retry.next_sleep().as_secs_f64());
        let retry = self.retry.sleep().await;
        MetricsRetryStream {
            retry,
            retries: self.retries,
            sleep_seconds: self.sleep_seconds,
            _finished: self._finished,
        }
    }
}
