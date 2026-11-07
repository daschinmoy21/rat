//! Micro-batch driver: source -> engine -> staged Parquet -> checkpoint commit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde::Serialize;

use crate::checkpoint::{Checkpoint, FORMAT_VERSION, Fingerprint, Position, Store};
use crate::engine::{Engine, LatePolicy, Output};
use crate::envelope::Envelope;
use crate::sink::{self, DtZone};

pub enum Fetch {
    /// Raw message values, in the order the engine must see them.
    Records(Vec<Vec<u8>>),
    /// Nothing available now. Try again after the trigger interval.
    Idle,
    /// Finite input exhausted.
    End,
}

pub trait Source {
    /// Live sources honor the configured minimum interval between batches.
    /// Finite replay remains unpaced so historical files can run at full speed.
    fn is_streaming(&self) -> bool {
        false
    }
    /// Position just past the last record handed out by `fetch`.
    fn position(&self) -> Position;
    fn fetch(&mut self, max: usize) -> Result<Fetch>;
    /// True once a shutdown was requested (signal). Checked between batches.
    fn should_stop(&mut self) -> bool {
        false
    }
    /// Wait for the trigger interval, waking early on shutdown.
    fn idle(&mut self, interval: Duration) {
        std::thread::sleep(interval);
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub sink_dir: PathBuf,
    pub checkpoint_dir: PathBuf,
    pub window_ms: i64,
    pub late_policy: LatePolicy,
    pub dt_zone: DtZone,
    pub batch_max: usize,
    pub trigger: Duration,
}

/// What one invocation did, printed as JSON by the CLI.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RunSummary {
    pub records_read: u64,
    pub records_rejected: u64,
    pub rejected_by_reason: BTreeMap<String, u64>,
    pub raw_rows: u64,
    pub correlated_rows: u64,
    pub late_dropped: u64,
    pub unjoinable: u64,
    pub batches: u64,
    pub files_written: u64,
    pub first_batch_id: u64,
    pub last_batch_id: u64,
    pub state_entries: usize,
    pub watermark_ms: Option<i64>,
    pub elapsed_ms: u128,
}

/// The engine as resumed from (or freshly created for) a checkpoint.
pub struct Resumed {
    pub store: Store,
    pub position: Position,
    checkpoint: Checkpoint,
}

pub fn resume(settings: &Settings, input: String) -> Result<Resumed> {
    let fingerprint = Fingerprint {
        window_ms: settings.window_ms,
        late_policy: settings.late_policy,
        dt_zone: settings.dt_zone,
        input,
    };
    let (store, existing) =
        Store::open(&settings.checkpoint_dir, &settings.sink_dir, &fingerprint)?;
    let checkpoint = existing.unwrap_or_else(|| Checkpoint {
        version: FORMAT_VERSION,
        fingerprint,
        batch_id: 0,
        position: Position::Start,
        engine: Default::default(),
        rejects: BTreeMap::new(),
        pending: Vec::new(),
    });
    Ok(Resumed {
        store,
        position: checkpoint.position.clone(),
        checkpoint,
    })
}

pub fn run(settings: &Settings, resumed: Resumed, source: &mut dyn Source) -> Result<RunSummary> {
    if settings.batch_max == 0 {
        bail!("batch size must be > 0");
    }
    let started = Instant::now();
    let Resumed {
        store,
        mut checkpoint,
        ..
    } = resumed;
    let mut engine = Engine::new(
        settings.window_ms,
        settings.late_policy,
        std::mem::take(&mut checkpoint.engine),
    );
    let before = engine.state().counters.clone();
    let mut summary = RunSummary {
        first_batch_id: checkpoint.batch_id + 1,
        last_batch_id: checkpoint.batch_id,
        ..Default::default()
    };
    let mut previous_batch_started: Option<Instant> = None;

    loop {
        if source.should_stop() {
            break;
        }
        if source.is_streaming() {
            if let Some(previous) = previous_batch_started {
                if let Some(remaining) = settings.trigger.checked_sub(previous.elapsed()) {
                    source.idle(remaining);
                }
                if source.should_stop() {
                    break;
                }
            }
            previous_batch_started = Some(Instant::now());
        }
        let records = match source.fetch(settings.batch_max)? {
            Fetch::Records(records) => records,
            Fetch::Idle => {
                // Persist a moved position even without records (e.g. resolved
                // `latest` offsets), as Spark writes its initial offsets at start.
                let position = source.position();
                if position != checkpoint.position {
                    let batch_id = checkpoint.batch_id + 1;
                    checkpoint.batch_id = batch_id;
                    checkpoint.position = position;
                    checkpoint.pending = Vec::new();
                    checkpoint.engine = engine.take_state();
                    let committed = store.commit(&checkpoint);
                    engine.restore(std::mem::take(&mut checkpoint.engine));
                    committed?;
                    summary.batches += 1;
                    summary.last_batch_id = batch_id;
                }
                source.idle(settings.trigger);
                continue;
            }
            Fetch::End => break,
        };
        let mut out = Output::default();
        for record in &records {
            match Envelope::parse(record) {
                Ok(env) => engine.process(env, &mut out),
                Err(reason) => {
                    *checkpoint
                        .rejects
                        .entry(reason.as_str().to_owned())
                        .or_default() += 1;
                    *summary
                        .rejected_by_reason
                        .entry(reason.as_str().to_owned())
                        .or_default() += 1;
                    summary.records_rejected += 1;
                }
            }
        }
        summary.records_read += records.len() as u64;
        engine.sweep();

        let batch_id = checkpoint.batch_id + 1;
        let staged = sink::stage_batch(
            &settings.sink_dir,
            batch_id,
            settings.dt_zone,
            &out.raw,
            &out.pairs,
        )?;
        summary.files_written += staged.len() as u64;
        checkpoint.batch_id = batch_id;
        checkpoint.position = source.position();
        checkpoint.pending = staged;
        checkpoint.engine = engine.take_state();
        let committed = store.commit(&checkpoint);
        engine.restore(std::mem::take(&mut checkpoint.engine));
        committed?;
        summary.batches += 1;
        summary.last_batch_id = batch_id;
    }

    let after = &engine.state().counters;
    summary.raw_rows = after.raw_rows - before.raw_rows;
    summary.correlated_rows = after.correlated_rows - before.correlated_rows;
    summary.late_dropped = after.late_dropped - before.late_dropped;
    summary.unjoinable = after.unjoinable - before.unjoinable;
    summary.state_entries = engine.state_entries();
    summary.watermark_ms = engine.watermark();
    summary.elapsed_ms = started.elapsed().as_millis();
    Ok(summary)
}

/// Refuse to wipe anything outside /tmp: `--reset` is for benchmarks and smoke runs.
pub fn reset_dirs(dirs: &[&Path]) -> Result<()> {
    // Validate the entire request before removing anything. Canonicalize the
    // nearest existing ancestor so /tmp/../home and symlinks cannot escape.
    let tmp_root = std::env::temp_dir().canonicalize()?;
    let system_tmp = Path::new("/tmp").canonicalize()?;
    for dir in dirs {
        let abs = std::path::absolute(dir)?;
        if abs
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            bail!("--reset refuses parent traversal in {}", abs.display());
        }
        let ancestor = abs
            .ancestors()
            .find(|p| p.exists())
            .unwrap_or(Path::new("/"));
        let resolved = ancestor.canonicalize()?.join(abs.strip_prefix(ancestor)?);
        let safe = [&system_tmp, &tmp_root]
            .iter()
            .any(|root| resolved != **root && resolved.starts_with(root));
        if !safe {
            bail!(
                "--reset refuses to delete {} (only paths under /tmp or $TMPDIR)",
                abs.display()
            );
        }
    }
    for dir in dirs {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
