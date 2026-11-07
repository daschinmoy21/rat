use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};

use rat_rs::engine::LatePolicy;
use rat_rs::generate::{GenOptions, generate};
use rat_rs::iggy::{IggyConfig, IggySource, Starting};
use rat_rs::pipeline::{self, Settings};
use rat_rs::sink::DtZone;
use rat_rs::source_file::FileSource;
use rat_rs::{digest, interval};

/// Correlate rat envelopes into partitioned Parquet, and query it.
#[derive(Parser)]
#[command(name = "rat-rs", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone)]
struct Common {
    /// Parquet root (events_raw/, correlated/).
    #[arg(long, env = "RAT_SINK_DIR", default_value = "/tmp/rat")]
    sink_dir: PathBuf,
    /// Checkpoint dir [default: $RAT_SINK_DIR/checkpoints/rat-rs].
    #[arg(long, env = "RAT_CHECKPOINT_DIR")]
    checkpoint_dir: Option<PathBuf>,
    /// Watermark delay and join half-window (Spark interval syntax).
    #[arg(long, env = "RAT_WATERMARK", default_value = "10 minutes")]
    watermark: String,
    /// `local` (legacy Spark default: JVM time zone) or `utc`, used to derive dt.
    #[arg(long, env = "RAT_DT_TIMEZONE", default_value = "local")]
    dt_timezone: DtZone,
    /// `drop`: late records skip the join, state is bounded.
    /// `keep`: no late-dropping or eviction (exact batch join, unbounded state).
    #[arg(long, env = "RAT_LATE_POLICY", default_value = "drop")]
    late_policy: LatePolicy,
    /// Max records per committed micro-batch.
    #[arg(long, env = "RAT_BATCH_MAX", default_value_t = 10_000)]
    batch_max: usize,
    /// Live micro-batch interval (also used when the broker is idle).
    #[arg(long, env = "RAT_TRIGGER", default_value = "1 second")]
    trigger: String,
}

impl Common {
    fn settings(&self) -> Result<Settings> {
        for path in std::iter::once(&self.sink_dir).chain(self.checkpoint_dir.iter()) {
            anyhow::ensure!(
                !path.as_os_str().to_string_lossy().contains("://"),
                "{} is a URI; rat-rs requires a local filesystem path (HDFS/object storage is not supported)",
                path.display()
            );
        }
        let sink_dir = std::path::absolute(&self.sink_dir)?;
        let checkpoint_dir = match &self.checkpoint_dir {
            Some(dir) => std::path::absolute(dir)?,
            None => sink_dir.join("checkpoints").join("rat-rs"),
        };
        let trigger = if self.trigger.trim().is_empty() {
            1_000
        } else {
            interval::parse_ms(&self.trigger)?
        };
        Ok(Settings {
            sink_dir,
            checkpoint_dir,
            window_ms: interval::parse_ms(&self.watermark)?,
            late_policy: self.late_policy,
            dt_zone: self.dt_timezone,
            batch_max: self.batch_max,
            trigger: Duration::from_millis(trigger.max(1) as u64),
        })
    }
}

#[derive(Args, Clone)]
struct GenArgs {
    #[arg(long, default_value_t = 100_000)]
    events: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Comma-separated source names.
    #[arg(
        long,
        default_value = "hn,lobsters,news,rss,stocks",
        value_delimiter = ','
    )]
    sources: Vec<String>,
    #[arg(long, default_value_t = 5_000)]
    entity_pool: u64,
    #[arg(long, default_value_t = 1_700_000_000_000)]
    start_ts_ms: i64,
    #[arg(long, default_value_t = 100)]
    step_ms: i64,
    /// Max backwards jitter (out-of-order but within the watermark by default).
    #[arg(long, default_value_t = 30_000)]
    disorder_ms: i64,
    #[arg(long, default_value_t = 0.2)]
    entityless: f64,
    /// Share of records made late by --late-by-ms.
    #[arg(long, default_value_t = 0.0)]
    late: f64,
    #[arg(long, default_value_t = 1_800_000)]
    late_by_ms: i64,
    /// Share of invalid lines.
    #[arg(long, default_value_t = 0.0)]
    invalid: f64,
}

impl From<&GenArgs> for GenOptions {
    fn from(a: &GenArgs) -> Self {
        GenOptions {
            events: a.events,
            seed: a.seed,
            sources: a.sources.clone(),
            entity_pool: a.entity_pool,
            start_ts_ms: a.start_ts_ms,
            step_ms: a.step_ms,
            disorder_ms: a.disorder_ms,
            entityless: a.entityless,
            late: a.late,
            late_by_ms: a.late_by_ms,
            invalid: a.invalid,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Write deterministic synthetic envelopes (JSONL).
    Gen {
        #[command(flatten)]
        gen_args: GenArgs,
        /// Output file [default: stdout].
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Replay a JSONL file through the streaming engine (resumes from checkpoint).
    Process {
        #[command(flatten)]
        common: Common,
        #[arg(long)]
        input: PathBuf,
        /// Keep running and pick up lines appended to the file.
        #[arg(long)]
        follow: bool,
        /// Delete sink + checkpoint first (only under /tmp).
        #[arg(long)]
        reset: bool,
        /// Skip the output digest (it reads every committed file back).
        #[arg(long)]
        no_digest: bool,
    },
    /// Consume Iggy over HTTP until SIGINT/SIGTERM.
    Consume {
        #[command(flatten)]
        common: Common,
        #[command(flatten)]
        iggy: IggyArgs,
    },
    /// Order-independent digest of the committed sink contents (works on Spark output too).
    Digest {
        #[arg(long, env = "RAT_SINK_DIR", default_value = "/tmp/rat")]
        sink_dir: PathBuf,
        /// Print every canonical row instead of the digest.
        #[arg(long)]
        rows: bool,
    },
    /// gen + process --reset in one step, prints timings and the digest.
    Bench {
        #[command(flatten)]
        gen_args: GenArgs,
        #[arg(long, default_value = "/tmp/rat-bench")]
        work_dir: PathBuf,
        #[arg(long, default_value_t = 10_000)]
        batch_max: usize,
        #[arg(long, default_value = "10 minutes")]
        watermark: String,
        #[arg(long, default_value = "utc")]
        dt_timezone: DtZone,
        #[arg(long, default_value = "drop")]
        late_policy: LatePolicy,
    },
    /// rat.Query parity: latest 10 raw rows and latest 10 correlated rows.
    #[cfg(feature = "sql")]
    Query {
        #[arg(long, env = "RAT_SINK_DIR", default_value = "/tmp/rat")]
        sink_dir: PathBuf,
    },
    /// Run SQL over tables rat_events and correlated.
    #[cfg(feature = "sql")]
    Sql {
        query: String,
        #[arg(long, env = "RAT_SINK_DIR", default_value = "/tmp/rat")]
        sink_dir: PathBuf,
        #[arg(long, default_value = "table")]
        format: rat_rs::query::Format,
    },
}

#[derive(Args, Clone)]
struct IggyArgs {
    #[arg(long, env = "RAT_IGGY_URL", default_value = "http://127.0.0.1:3000")]
    iggy_url: String,
    #[arg(long, env = "RAT_IGGY_USERNAME", default_value = "iggy")]
    iggy_username: String,
    #[arg(
        long,
        env = "RAT_IGGY_PASSWORD",
        default_value = "iggy",
        hide_env_values = true
    )]
    iggy_password: String,
    #[arg(long, env = "RAT_IGGY_PAT", hide_env_values = true)]
    iggy_pat: Option<String>,
    #[arg(long, env = "RAT_IGGY_STREAM", default_value = "rat")]
    iggy_stream: String,
    #[arg(long, env = "RAT_IGGY_TOPIC_PATTERN", default_value = r"events\..*")]
    iggy_topic_pattern: String,
    #[arg(long, env = "RAT_IGGY_TOPIC_EXCLUDE", default_value = r"\.dlq$")]
    iggy_topic_exclude: String,
    #[arg(long, env = "RAT_IGGY_POLL_COUNT", default_value_t = 1000)]
    iggy_poll_count: u32,
    #[arg(long, env = "RAT_IGGY_CONSUMER_ID", default_value = "rat-engine")]
    iggy_consumer_id: String,
    #[arg(long, env = "RAT_STARTING_OFFSETS", default_value = "latest")]
    starting_offsets: Starting,
}

fn main() {
    if let Err(e) = run(Cli::parse()) {
        eprintln!("rat-rs: error: {e:#}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Gen { gen_args, out } => {
            let opts = GenOptions::from(&gen_args);
            match out {
                Some(path) => {
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let mut w = BufWriter::new(File::create(&path)?);
                    generate(&opts, &mut w)?;
                }
                None => generate(&opts, &mut BufWriter::new(std::io::stdout().lock()))?,
            }
            Ok(())
        }
        Command::Process {
            common,
            input,
            follow,
            reset,
            no_digest,
        } => {
            let settings = common.settings()?;
            let summary = process_file(&settings, &input, follow, reset, !no_digest)?;
            print_json(&summary)
        }
        Command::Consume { common, iggy } => {
            let settings = common.settings()?;
            let cfg = IggyConfig {
                url: iggy.iggy_url,
                username: iggy.iggy_username,
                password: iggy.iggy_password,
                pat: iggy.iggy_pat.filter(|p| !p.is_empty()),
                stream: iggy.iggy_stream.clone(),
                topic_pattern: iggy.iggy_topic_pattern,
                topic_exclude: iggy.iggy_topic_exclude,
                poll_count: iggy.iggy_poll_count.max(1),
                consumer_id: iggy.iggy_consumer_id,
                starting: iggy.starting_offsets,
                request_timeout: Duration::from_secs(30),
            };
            let resumed = pipeline::resume(&settings, format!("iggy:{}", iggy.iggy_stream))?;
            let mut source = IggySource::open(cfg, &resumed.position)?;
            eprintln!(
                "rat-rs: consuming iggy stream {:?} into {} (checkpoint {})",
                iggy.iggy_stream,
                settings.sink_dir.display(),
                settings.checkpoint_dir.display()
            );
            let summary = pipeline::run(&settings, resumed, &mut source)?;
            let mut value = serde_json::to_value(&summary)?;
            value["iggy"] = serde_json::to_value(source.stats())?;
            print_json(&value)
        }
        Command::Digest { sink_dir, rows } => {
            if rows {
                let (raw, corr, _) = digest::canonical_rows(&sink_dir)?;
                let mut out = BufWriter::new(std::io::stdout().lock());
                for row in raw.iter().chain(corr.iter()) {
                    writeln!(out, "{row}")?;
                }
                return Ok(out.flush()?);
            }
            print_json(&digest::compute(&sink_dir)?)
        }
        Command::Bench {
            gen_args,
            work_dir,
            batch_max,
            watermark,
            dt_timezone,
            late_policy,
        } => {
            let input = work_dir.join("input.jsonl");
            std::fs::create_dir_all(&work_dir)?;
            let started = Instant::now();
            generate(
                &GenOptions::from(&gen_args),
                &mut BufWriter::new(File::create(&input)?),
            )?;
            let gen_ms = started.elapsed().as_millis();
            let common = Common {
                sink_dir: work_dir.join("sink"),
                checkpoint_dir: Some(work_dir.join("checkpoint")),
                watermark,
                dt_timezone,
                late_policy,
                batch_max,
                trigger: "1 second".into(),
            };
            let settings = common.settings()?;
            let mut summary = process_file(&settings, &input, false, true, true)?;
            let input_bytes = std::fs::metadata(&input)?.len();
            let process_ms = summary["elapsed_ms"].as_u64().unwrap_or(0).max(1);
            summary["gen_ms"] = json!(gen_ms);
            summary["input"] = json!(input);
            summary["input_bytes"] = json!(input_bytes);
            summary["records_per_sec"] =
                json!((summary["records_read"].as_u64().unwrap_or(0) * 1000) / process_ms);
            print_json(&summary)
        }
        #[cfg(feature = "sql")]
        Command::Query { sink_dir } => block_on(async move {
            let ctx = rat_rs::query::context(&sink_dir).await?;
            for (title, sql) in rat_rs::query::LEGACY_QUERIES {
                println!("{title}");
                let batches = rat_rs::query::sql(&ctx, sql).await?;
                println!(
                    "{}",
                    rat_rs::query::render(&batches, rat_rs::query::Format::Table)?
                );
            }
            Ok(())
        }),
        #[cfg(feature = "sql")]
        Command::Sql {
            query,
            sink_dir,
            format,
        } => block_on(async move {
            let ctx = rat_rs::query::context(&sink_dir).await?;
            let batches = rat_rs::query::sql(&ctx, &query).await?;
            print!("{}", rat_rs::query::render(&batches, format)?);
            if format == rat_rs::query::Format::Table {
                println!();
            }
            Ok(())
        }),
    }
}

fn process_file(
    settings: &Settings,
    input: &Path,
    follow: bool,
    reset: bool,
    with_digest: bool,
) -> Result<Value> {
    if reset {
        pipeline::reset_dirs(&[&settings.sink_dir, &settings.checkpoint_dir])?;
    }
    let canonical = input
        .canonicalize()
        .with_context(|| format!("input {}", input.display()))?;
    let resumed = pipeline::resume(settings, format!("file:{}", canonical.display()))?;
    let mut source = FileSource::open(&canonical, &resumed.position, follow)?;
    let summary = pipeline::run(settings, resumed, &mut source)?;
    let mut value = serde_json::to_value(&summary)?;
    if with_digest {
        let d = digest::compute(&settings.sink_dir)?;
        value["output_digest"] = json!(d.digest);
        value["sink_raw_rows"] = json!(d.raw_rows);
        value["sink_correlated_rows"] = json!(d.correlated_rows);
        value["sink_files"] = json!(d.files);
    }
    value["sink_dir"] = json!(settings.sink_dir);
    value["checkpoint_dir"] = json!(settings.checkpoint_dir);
    Ok(value)
}

#[cfg(feature = "sql")]
fn block_on(fut: impl std::future::Future<Output = Result<()>>) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(fut)
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}
