//! Ichnos: one binary, several modes.
//!
//! `serve` answers requests, `worker` drains the queue and writes, and the rest
//! are operational tools. Splitting them into separate processes rather than
//! threads is what lets the read path and the write path be scaled apart.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ichnos::api::auth::KeyCache;
use ichnos::api::handlers::{router, AppState};
use ichnos::api::quota::{MemoryQuota, QuotaCounter};
use ichnos::config::Config;
use ichnos::ingest::lease::Leases;
use ichnos::ingest::queue::{MemoryQueue, Queue};
use ichnos::redis_backend::{RedisCache, RedisLeases, RedisQueue, RedisQuota};
use ichnos::store::cache::{CachedStore, MemoryCache};
use ichnos::store::cql::CqlStore;
use ichnos::store::memory::MemoryStore;
use ichnos::store::{ApiKey, Store};
use ichnos::worker::Worker;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "ichnos", version, about = "Geolocation from WiFi and cell towers")]
struct Cli {
    /// Configuration file. Without one, defaults apply and `ICHNOS__` variables
    /// override them.
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Run without any external dependency, keeping everything in memory. For
    /// development and for trying the service out; nothing survives a restart.
    #[arg(long, global = true)]
    in_memory: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Answer requests.
    Serve {
        /// Also run a worker in this process.
        ///
        /// For development and small deployments. In production the two are
        /// separate so the read path and the write path can be scaled apart,
        /// and so a busy worker cannot slow down an answer.
        #[arg(long)]
        with_worker: bool,
    },
    /// Drain the queue and write what it learns.
    Worker {
        /// Name used in the lease registry. Defaults to the hostname, which is
        /// what distinguishes one pod from another.
        #[arg(long)]
        identity: Option<String>,
    },
    /// Replay recorded reports through the pipeline and print the result.
    ///
    /// Deterministic and free of I/O, so a week of traffic takes seconds. This
    /// is how a change to the maths is measured before it is deployed.
    Replay {
        /// A file of newline-delimited submit bodies.
        input: PathBuf,
        /// Where to write the resulting stations, as newline-delimited JSON.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Load stations from a newline-delimited JSON file.
    Import { input: PathBuf },
    /// Write every station to a newline-delimited JSON file.
    ///
    /// In-memory runs only. Exporting every row means scanning, which the
    /// production store does not offer: it is a key-value store on purpose.
    /// Take production snapshots with the database's own tooling, which does
    /// not drag the whole table through this service.
    Export { output: PathBuf },
    /// Manage API keys.
    Keys {
        #[command(subcommand)]
        action: KeyAction,
    },
    /// Print the effective configuration and exit.
    ///
    /// Useful for seeing what the environment actually did to the defaults.
    ShowConfig,
}

#[derive(Subcommand)]
enum KeyAction {
    /// Create or replace a key.
    Add {
        key: String,
        #[arg(long, default_value = "")]
        name: String,
        #[arg(long)]
        locate: bool,
        #[arg(long)]
        submit: bool,
        #[arg(long)]
        admin: bool,
    },
    /// Show one key.
    Show { key: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing();

    let cfg = Config::load(cli.config.as_deref()).context("loading configuration")?;

    match cli.command {
        Command::ShowConfig => {
            println!("{}", toml::to_string_pretty(&cfg)?);
            Ok(())
        }
        Command::Serve { with_worker } => serve(cfg, cli.in_memory, with_worker).await,
        Command::Worker { identity } => run_worker(cfg, cli.in_memory, identity).await,
        Command::Replay { input, output } => {
            ichnos::replay::run(&cfg, &input, output.as_deref()).await
        }
        Command::Import { input } => import(cfg, cli.in_memory, &input).await,
        Command::Export { output } => export(cfg, cli.in_memory, &output).await,
        Command::Keys { action } => keys(cfg, cli.in_memory, action).await,
    }
}

/// Structured logs by default, because they are read by machines far more often
/// than by people.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("ICHNOS_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("ICHNOS_LOG_FORMAT").as_deref() == Ok("pretty") {
        builder.init();
    } else {
        builder.json().flatten_event(true).init();
    }
}

/// Assemble the storage stack: the real store, wrapped in a cache.
async fn build_store(cfg: &Config, in_memory: bool) -> Result<Arc<dyn Store>> {
    if in_memory {
        warn!("running in memory: nothing will survive a restart");
        let store = MemoryStore::new();
        // A development run should not also need an API key to exist.
        store.put_api_key(&ApiKey::full("dev-key")).await.ok();
        return Ok(Arc::new(CachedStore::new(store, MemoryCache::new(), cfg.cache.clone())));
    }

    let store = CqlStore::connect(&cfg.cql).await.context("connecting to the database")?;

    match RedisCache::connect(&cfg.redis.cache_url).await {
        Ok(cache) => Ok(Arc::new(CachedStore::new(store, cache, cfg.cache.clone()))),
        Err(e) => {
            // A missing cache makes reads slower, not impossible, so it is not
            // worth refusing to start over.
            warn!(error = %e, "no cache available; every read will reach the database");
            Ok(Arc::new(CachedStore::new(store, MemoryCache::new(), cfg.cache.clone())))
        }
    }
}

async fn build_queue(cfg: &Config, in_memory: bool, consumer: String) -> Result<Arc<dyn Queue>> {
    if in_memory {
        return Ok(Arc::new(MemoryQueue::new(cfg.ingest.shards, cfg.ingest.stream_maxlen)));
    }
    let queue = RedisQueue::connect(
        &cfg.redis.queue_url,
        cfg.ingest.shards,
        cfg.ingest.stream_maxlen,
        consumer,
    )
    .await
    .map_err(|e| anyhow::anyhow!("connecting to the queue: {e}"))?;
    Ok(Arc::new(queue))
}

async fn serve(cfg: Config, in_memory: bool, with_worker: bool) -> Result<()> {
    let store = build_store(&cfg, in_memory).await?;
    let queue = build_queue(&cfg, in_memory, format!("api-{}", identity())).await?;
    let keys = Arc::new(KeyCache::new(&cfg.api));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    if with_worker {
        // The in-memory store and queue live in this process, so a separate
        // worker process would find them empty. Sharing them is the only way
        // a single-process run can work at all.
        info!("running a worker in this process as well");
        let leases: Arc<dyn Leases> = if in_memory {
            Arc::new(ichnos::ingest::lease::MemoryLeases::new())
        } else {
            Arc::new(
                RedisLeases::connect(&cfg.redis.queue_url)
                    .await
                    .map_err(|e| anyhow::anyhow!("connecting to the lease registry: {e}"))?,
            )
        };
        let mut worker = Worker::new(
            queue.clone(),
            store.clone(),
            leases,
            cfg.clone(),
            format!("inline-{}", identity()),
        );
        worker.announce().await.map_err(|e| anyhow::anyhow!("announcing: {e}"))?;
        worker.claim_shards().await.map_err(|e| anyhow::anyhow!("claiming shards: {e}"))?;
        let rx = shutdown_rx.clone();
        tokio::spawn(async move { worker.run(rx).await });
    }

    // Counting in Redis keeps one allowance per key across every pod. The
    // in-memory fallback counts per process, which is honest for a single-node
    // run and useless for a fleet, so it is only used when there is no Redis.
    let quota: Arc<dyn QuotaCounter> = if in_memory {
        Arc::new(MemoryQuota::new())
    } else {
        match RedisQuota::connect(&cfg.redis.cache_url).await {
            Ok(counter) => Arc::new(counter),
            Err(e) => {
                warn!(error = %e, "no quota counter available; allowances will not be enforced");
                Arc::new(MemoryQuota::new())
            }
        }
    };

    let bind = cfg.server.bind.clone();
    let state = AppState { store, queue, cfg: Arc::new(cfg), keys, quota };

    let listener =
        tokio::net::TcpListener::bind(&bind).await.with_context(|| format!("binding {bind}"))?;
    info!(address = %bind, "serving");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        })
        .await
        .context("serving")
}

async fn run_worker(cfg: Config, in_memory: bool, identity_arg: Option<String>) -> Result<()> {
    let identity = identity_arg.unwrap_or_else(identity);
    let store = build_store(&cfg, in_memory).await?;
    let queue = build_queue(&cfg, in_memory, identity.clone()).await?;

    let leases: Arc<dyn Leases> = if in_memory {
        Arc::new(ichnos::ingest::lease::MemoryLeases::new())
    } else {
        Arc::new(
            RedisLeases::connect(&cfg.redis.queue_url)
                .await
                .map_err(|e| anyhow::anyhow!("connecting to the lease registry: {e}"))?,
        )
    };

    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = tx.send(true);
    });

    let mut worker = Worker::new(queue, store, leases, cfg, identity);
    // Announce before claiming, or this worker counts itself as the only one
    // and takes every shard before its siblings are visible.
    worker.announce().await.map_err(|e| anyhow::anyhow!("announcing: {e}"))?;
    worker.claim_shards().await.map_err(|e| anyhow::anyhow!("claiming shards: {e}"))?;
    worker.run(rx).await;
    Ok(())
}

async fn import(cfg: Config, in_memory: bool, input: &std::path::Path) -> Result<()> {
    let store = build_store(&cfg, in_memory).await?;
    let text = std::fs::read_to_string(input).with_context(|| format!("reading {input:?}"))?;

    let mut batch = Vec::new();
    let mut loaded = 0usize;
    let mut skipped = 0usize;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        match serde_json::from_str::<ichnos::model::Station>(line) {
            Ok(station) => batch.push(station),
            Err(_) => skipped += 1,
        }
        if batch.len() >= 1_000 {
            store.put_stations(&batch).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            loaded += batch.len();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.put_stations(&batch).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        loaded += batch.len();
    }

    info!(loaded, skipped, "import finished");
    println!("imported {loaded} stations, skipped {skipped} malformed lines");
    Ok(())
}

async fn export(_cfg: Config, in_memory: bool, output: &std::path::Path) -> Result<()> {
    // Exporting every row means scanning, which the production store does not
    // offer: it is a key-value store on purpose. Snapshots come from the
    // database's own tooling instead.
    if !in_memory {
        anyhow::bail!(
            "export works against an in-memory store only. For a production snapshot, use the \
             database's own export tooling, which does not scan through this service."
        );
    }
    let store = MemoryStore::new();
    let mut file = std::fs::File::create(output).with_context(|| format!("creating {output:?}"))?;
    use std::io::Write;
    for station in store.snapshot() {
        writeln!(file, "{}", serde_json::to_string(&station)?)?;
    }
    Ok(())
}

async fn keys(cfg: Config, in_memory: bool, action: KeyAction) -> Result<()> {
    let store = build_store(&cfg, in_memory).await?;
    match action {
        KeyAction::Add { key, name, locate, submit, admin } => {
            let api_key = ApiKey {
                key: key.clone(),
                name,
                enabled: true,
                allow_locate: locate,
                allow_submit: submit,
                allow_admin: admin,
                daily_quota: cfg.api.default_daily_quota,
            };
            store.put_api_key(&api_key).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("added key {key}: locate={locate} submit={submit} admin={admin}");
        }
        KeyAction::Show { key } => match store.get_api_key(&key).await {
            Ok(Some(found)) => println!("{}", serde_json::to_string_pretty(&found)?),
            Ok(None) => println!("no such key"),
            Err(e) => anyhow::bail!("{e}"),
        },
    }
    Ok(())
}

/// What this process calls itself in the lease registry.
fn identity() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .unwrap_or_else(|_| format!("local-{}", std::process::id()))
}

/// Stop on either signal Kubernetes might send.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
