//! Durable checkpoint and the commit protocol that makes sink output
//! exactly-once across crashes and restarts.
//!
//! For each micro-batch:
//! 1. Rows are written to hidden `.part-*.tmp` files in their final partition
//!    directories, then fsynced. Readers ignore them.
//! 2. `checkpoint.json` is replaced atomically (write tmp, fsync, rename,
//!    fsync dir). It holds the new input position, the engine state, and the
//!    list of staged files. **This rename is the commit point.**
//! 3. Each staged file is renamed to its final `part-*.parquet` name.
//!
//! Recovery: when the checkpoint lists staged files, any tmp file that still
//! exists is renamed (roll forward). Any other `.tmp` file belongs to a batch
//! that never committed and is deleted. That batch is then re-read from the
//! committed position. The engine is deterministic, so the re-read produces
//! the same rows.
//!
//! An exclusive lock on `LOCK` keeps two writers off the same checkpoint.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::engine::{EngineState, LatePolicy};
use crate::sink::{self, CORRELATED_DIR, DtZone, RAW_DIR, StagedFile, TMP_SUFFIX};

pub const FORMAT_VERSION: u32 = 1;
const FILE_NAME: &str = "checkpoint.json";

/// Settings that change output. A checkpoint written with one set cannot be
/// resumed with another: the earlier output would not match the new settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fingerprint {
    pub window_ms: i64,
    pub late_policy: LatePolicy,
    pub dt_zone: DtZone,
    /// `file:<canonical path>` or `iggy:<stream>`.
    pub input: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
// Externally tagged: internal tagging buffers values and then cannot
// parse the numeric partition-id map keys back from JSON strings.
#[serde(rename_all = "snake_case")]
pub enum Position {
    #[default]
    Start,
    File {
        byte_offset: u64,
        line: u64,
    },
    Iggy {
        /// topic name -> partition id -> next offset to read.
        offsets: BTreeMap<String, BTreeMap<u32, u64>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub fingerprint: Fingerprint,
    /// Last committed batch id. 0 means nothing committed yet.
    pub batch_id: u64,
    pub position: Position,
    pub engine: EngineState,
    /// Rejected record counts by reason, cumulative.
    pub rejects: BTreeMap<String, u64>,
    /// Files staged by `batch_id`, renamed after the commit (see module docs).
    pub pending: Vec<StagedFile>,
}

pub struct Store {
    dir: PathBuf,
    sink_dir: PathBuf,
    _lock: File,
}

impl Store {
    /// Open the store, lock it, and run crash recovery. Returns `None` when
    /// there is no checkpoint yet (a fresh start).
    pub fn open(
        dir: &Path,
        sink_dir: &Path,
        fingerprint: &Fingerprint,
    ) -> Result<(Store, Option<Checkpoint>)> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        fs::create_dir_all(sink_dir).with_context(|| format!("create {}", sink_dir.display()))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))?;
        if lock.try_lock().is_err() {
            bail!(
                "checkpoint {} is locked by another rat-rs process",
                dir.display()
            );
        }
        let store = Store {
            dir: dir.to_path_buf(),
            sink_dir: sink_dir.to_path_buf(),
            _lock: lock,
        };
        let path = dir.join(FILE_NAME);
        let checkpoint = match fs::read(&path) {
            Ok(bytes) => {
                let cp: Checkpoint = serde_json::from_slice(&bytes)
                    .with_context(|| format!("corrupt checkpoint {}", path.display()))?;
                if cp.version != FORMAT_VERSION {
                    bail!(
                        "checkpoint format {} is not supported (want {FORMAT_VERSION})",
                        cp.version
                    );
                }
                if &cp.fingerprint != fingerprint {
                    bail!(
                        "checkpoint {} was written with different settings\n  stored: {:?}\n  now:    {:?}\nuse a new RAT_CHECKPOINT_DIR + RAT_SINK_DIR, or --reset",
                        path.display(),
                        cp.fingerprint,
                        fingerprint
                    );
                }
                Some(cp)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        store.recover(checkpoint.as_ref())?;
        Ok((store, checkpoint))
    }

    fn recover(&self, checkpoint: Option<&Checkpoint>) -> Result<()> {
        if let Some(cp) = checkpoint {
            finish_renames(&cp.pending)?;
        }
        for stray in
            self.sink_files(|name| name.starts_with(".part-") && name.ends_with(TMP_SUFFIX))?
        {
            fs::remove_file(&stray).with_context(|| format!("remove {}", stray.display()))?;
        }
        if checkpoint.is_none() {
            let existing = self.sink_files(|name| name.ends_with(".parquet"))?;
            if let Some(first) = existing.first() {
                bail!(
                    "no checkpoint in {} but the sink already holds data ({}); resuming would duplicate output. \
                     Point RAT_CHECKPOINT_DIR at the matching checkpoint, or use --reset",
                    self.dir.display(),
                    first.display()
                );
            }
        }
        Ok(())
    }

    /// Commit point for one batch: persist `next` (whose `pending` lists the
    /// already-staged files), then make those files visible.
    pub fn commit(&self, next: &Checkpoint) -> Result<()> {
        let bytes = serde_json::to_vec(next)?;
        let tmp = self.dir.join(format!("{FILE_NAME}{TMP_SUFFIX}"));
        {
            let mut file = File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, self.dir.join(FILE_NAME))?;
        sink::sync_dir(&self.dir)?;
        finish_renames(&next.pending)
    }

    fn sink_files(&self, pred: fn(&str) -> bool) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for table in [RAW_DIR, CORRELATED_DIR] {
            out.extend(sink::walk_files(
                &self.sink_dir.join(table),
                &|p: &Path| p.file_name().and_then(|n| n.to_str()).is_some_and(pred),
            )?);
        }
        Ok(out)
    }
}

fn finish_renames(staged: &[StagedFile]) -> Result<()> {
    let mut dirs: Vec<&Path> = Vec::new();
    for file in staged {
        if file.tmp.exists() {
            fs::rename(&file.tmp, &file.fin)
                .with_context(|| format!("rename {}", file.tmp.display()))?;
        } else if !file.fin.exists() {
            bail!(
                "committed file {} is missing (neither tmp nor final exists): sink was modified outside rat-rs",
                file.fin.display()
            );
        }
        // Partition dirs (dt=, source=) may be new: sync up to the table dir.
        for parent in file.fin.ancestors().skip(1).take(3) {
            if !dirs.contains(&parent) {
                dirs.push(parent);
            }
        }
    }
    for dir in dirs {
        sink::sync_dir(dir)?;
    }
    Ok(())
}
