/// Shared test fixtures for integration tests.
///
/// Keeping container bootstrapping in one place avoids duplicating the setup
/// logic across the Postgres, Redis, and HTTP integration suites.

mod mock_rpc;

use std::{collections::HashMap, time::Duration};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::{postgres::Postgres, redis::Redis};

use crate::config::Config;
use crate::price_cache::PriceCache;
use crate::redis_cache::RedisCache;

pub use mock_rpc::MockRpcServer;

/// Start a temporary Postgres container and return a SQLx pool bound to it.
///
/// Callers should keep the returned container alive for the duration of the
/// test and close the pool before dropping the container.
#[allow(dead_code)]
pub async fn setup_postgres() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to test postgres");

    (pool, container)
}

/// Start a temporary Redis container and return a cache client bound to it.
///
/// A short delay gives the connection manager time to fully initialize before
/// the tests begin issuing commands.
#[allow(dead_code)]
pub async fn setup_redis() -> (RedisCache, testcontainers::ContainerAsync<Redis>) {
    let container = Redis::default().start().await.expect("redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("redis port");
    let url = format!("redis://127.0.0.1:{port}");

    let cache = RedisCache::new(&url).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    (cache, container)
}

/// Default asset prices used to satisfy health-check readiness in tests.
#[allow(dead_code)]
pub fn default_test_prices() -> HashMap<String, f64> {
    HashMap::from([
        ("BTC".to_string(), 60_000.0),
        ("ETH".to_string(), 3_000.0),
        ("XLM".to_string(), 0.12),
    ])
}

/// Start a mock Stellar RPC server and return a test [`Config`] + populated cache.
///
/// Call [`MockRpcServer::shutdown`] when the test finishes so the ephemeral port
/// is released before the next test runs.
#[allow(dead_code)]
pub async fn setup_healthy_test_env() -> (Config, PriceCache, MockRpcServer) {
    let mock = MockRpcServer::start().await;
    let mut config = Config::default_for_test();
    config.stellar_rpc_url = mock.url();

    let cache = PriceCache::new();
    cache.update(default_test_prices());

    (config, cache, mock)
}

/// Documented upper bound for how long services may take to recover after a
/// dependency (Postgres, Redis, or RPC) comes back online.
///
/// Chaos scenarios assert recovery within this bound so the number is shared
/// between the fault-injection suite and the service health-check documentation.
#[allow(dead_code)]
pub const DEPENDENCY_RECOVERY_BOUND: Duration = Duration::from_secs(10);

/// Maximum latency injected into a dependency during degraded-operation tests.
///
/// Services must continue to make progress (or fail fast and retry) while a
/// dependency is slower than this threshold.
#[allow(dead_code)]
pub const MAX_INJECTED_LATENCY: Duration = Duration::from_millisec(500);

/// Minimum duration a fault is held in place before recovery is exercised.
///
/// Keeping the fault active long enough ensures the system actually observes
/// degraded behavior rather than racing past a transient blip.
#[allow(dead_code)]
pub const MIN_FAULT_DURATION: Duration = Duration::from_milliseconds(500);

/// Represents the kind of fault injected into a dependency during a chaos test.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// Add artificial latency to requests.
    Latency,
    /// Reset in-flight connections.
    ConnectionReset,
    /// Throttle requests (e.g. return 429 / reject over a limit).
    Throttling,
    /// Fail completely (connection refused).
    Outage,
}

/// Metrics captured around a fault-injection window for reporting.
///
/// These are recorded by the chaos suite so recovery behaviour is repeatable
/// and auditable rather than hidden in ad-hoc test logic.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct RecoveryMetrics {
    /// Number of events accepted by ingestion during the fault window.
    pub events_ingested: u64,
    /// Number of events durably persisted after recovery.
    pub events_persisted: u64,
    /// Number of duplicate transactions detected after recovery.
    pub duplicate_transactions: u64,
    /// How long the system took to resume normal operation after the fault ended.
    pub recovery_time: Duration,
}

impl RecoveryMetrics {
    /// Returns true when no events were lost and no duplicate transactions
    /// occurred, and recovery completed within the documented bound.
    [#allow(dead_code)]
    pub fn is_healthy(&self) -> bool {
        self.events_ingested == self.events_persisted
            && self.duplicate_transactions == 0
            && self.recovery_time <= DEPENDENCY_RECOVERY_BOUND
    }
}
