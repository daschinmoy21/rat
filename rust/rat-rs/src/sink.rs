//! Partitioned Parquet sinks, laid out the way Spark's file sink lays them
//! out, so the existing Hive DDL (`hive/rat_events.sql`) can still mount them:
//!
//! ```text
//! $RAT_SINK_DIR/events_raw/dt=YYYY-MM-DD/source=<src>/part-<batch>-<n>.parquet
//! $RAT_SINK_DIR/correlated/dt=YYYY-MM-DD/part-<batch>-<n>.parquet
//! ```
//!
//! Partition columns are stored only in the directory names, as Spark does.
//! Files are written under a `.tmp` name in their final directory and become
//! visible only once the checkpoint has committed them (see `checkpoint.rs`).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, Local, NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::engine::{PairRow, RawRow};

pub const RAW_DIR: &str = "events_raw";
pub const CORRELATED_DIR: &str = "correlated";
pub const TMP_SUFFIX: &str = ".tmp";
/// Hive/Spark name for a null or empty partition value.
pub const DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Time zone used to derive `dt` from `ts_ms`. Spark uses the session time
/// zone, which defaults to the JVM time zone, which is the host's local zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DtZone {
    Local,
    Utc,
}

impl std::str::FromStr for DtZone {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "local" => Ok(DtZone::Local),
            "utc" => Ok(DtZone::Utc),
            other => Err(format!("dt timezone must be local|utc, got {other:?}")),
        }
    }
}

impl DtZone {
    pub fn date(self, ts_ms: i64) -> NaiveDate {
        let utc: DateTime<Utc> = Utc
            .timestamp_millis_opt(ts_ms)
            .single()
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        match self {
            DtZone::Utc => utc.date_naive(),
            DtZone::Local => utc.with_timezone(&Local).date_naive(),
        }
    }
}

pub fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, true),
        Field::new(
            "entities",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
        Field::new("ts_ms", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

pub fn correlated_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("a_id", DataType::Utf8, true),
        Field::new("b_id", DataType::Utf8, true),
        Field::new("ts_ms", DataType::Int64, true),
    ]))
}

/// A file written under its temp name, waiting for commit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedFile {
    pub tmp: PathBuf,
    pub fin: PathBuf,
    pub rows: u64,
}

/// Write one batch's rows as temp files, one per partition. Rows keep input
/// order inside each file. Returns the staged files (already fsynced).
pub fn stage_batch(
    sink_dir: &Path,
    batch_id: u64,
    zone: DtZone,
    raw: &[RawRow],
    pairs: &[PairRow],
) -> Result<Vec<StagedFile>> {
    let mut staged = Vec::new();

    let mut raw_parts: BTreeMap<(NaiveDate, String), Vec<&RawRow>> = BTreeMap::new();
    for row in raw {
        let source = match row.source.as_deref() {
            None | Some("") => DEFAULT_PARTITION.to_owned(),
            Some(s) => escape_path_name(s),
        };
        raw_parts
            .entry((zone.date(row.ts_ms), source))
            .or_default()
            .push(row);
    }
    for (seq, ((dt, source), rows)) in raw_parts.into_iter().enumerate() {
        let dir = sink_dir
            .join(RAW_DIR)
            .join(format!("dt={dt}"))
            .join(format!("source={source}"));
        let batch = raw_batch(&rows)?;
        staged.push(write_staged(&dir, batch_id, seq, &batch)?);
    }

    let mut pair_parts: BTreeMap<NaiveDate, Vec<&PairRow>> = BTreeMap::new();
    for row in pairs {
        pair_parts
            .entry(zone.date(row.ts_ms))
            .or_default()
            .push(row);
    }
    for (seq, (dt, rows)) in pair_parts.into_iter().enumerate() {
        let dir = sink_dir.join(CORRELATED_DIR).join(format!("dt={dt}"));
        let batch = pair_batch(&rows)?;
        staged.push(write_staged(&dir, batch_id, seq, &batch)?);
    }
    Ok(staged)
}

/// Spark `ExternalCatalogUtils.escapePathName` (non-Windows character set).
pub fn escape_path_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        let needs = matches!(
            ch,
            '\u{01}'
                ..='\u{1F}'
                    | '"'
                    | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '\u{7F}'
                    | '{'
                    | '['
                    | ']'
                    | '^'
        );
        if needs {
            out.push_str(&format!("%{:02X}", ch as u32));
        } else {
            out.push(ch);
        }
    }
    out
}

/// Inverse of [`escape_path_name`].
pub fn unescape_path_name(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(hi * 16 + lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    (byte as char).to_digit(16).map(|d| d as u8)
}

fn raw_batch(rows: &[&RawRow]) -> Result<RecordBatch> {
    let event_id: StringArray = rows.iter().map(|r| Some(r.event_id.as_str())).collect();
    let mut entities = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "element",
        DataType::Utf8,
        true,
    )));
    for row in rows {
        match &row.entities {
            None => entities.append_null(),
            Some(list) => {
                for item in list {
                    entities.values().append_option(item.as_deref());
                }
                entities.append(true);
            }
        }
    }
    let ts: Int64Array = rows.iter().map(|r| Some(r.ts_ms)).collect();
    let payload: StringArray = rows.iter().map(|r| r.payload.as_deref()).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(event_id),
        Arc::new(entities.finish()),
        Arc::new(ts),
        Arc::new(payload),
    ];
    Ok(RecordBatch::try_new(raw_schema(), columns)?)
}

fn pair_batch(rows: &[&PairRow]) -> Result<RecordBatch> {
    let entity: StringArray = rows.iter().map(|r| Some(r.entity.as_str())).collect();
    let a: StringArray = rows.iter().map(|r| Some(r.a_id.as_str())).collect();
    let b: StringArray = rows.iter().map(|r| Some(r.b_id.as_str())).collect();
    let ts: Int64Array = rows.iter().map(|r| Some(r.ts_ms)).collect();
    let columns: Vec<ArrayRef> = vec![Arc::new(entity), Arc::new(a), Arc::new(b), Arc::new(ts)];
    Ok(RecordBatch::try_new(correlated_schema(), columns)?)
}

fn write_staged(dir: &Path, batch_id: u64, seq: usize, batch: &RecordBatch) -> Result<StagedFile> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let name = format!("part-{batch_id:012}-{seq:05}.snappy.parquet");
    let fin = dir.join(&name);
    let tmp = dir.join(format!(".{name}{TMP_SUFFIX}"));
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("rat-rs".to_owned())
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    let file = writer.into_inner()?;
    file.sync_all()
        .with_context(|| format!("fsync {}", tmp.display()))?;
    Ok(StagedFile {
        tmp,
        fin,
        rows: batch.num_rows() as u64,
    })
}

/// fsync a directory so a rename or create inside it is durable.
pub fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync dir {}", dir.display()))
}

/// Every file under `root` (recursively) matching `pred`.
pub fn walk_files(root: &Path, pred: &dyn Fn(&Path) -> bool) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("read dir {}", dir.display())),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                stack.push(path);
            } else if pred(&path) {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_round_trip() {
        assert_eq!(escape_path_name("hn"), "hn");
        assert_eq!(escape_path_name("a/b=c:d%"), "a%2Fb%3Dc%3Ad%25");
        assert_eq!(unescape_path_name("a%2Fb%3Dc%3Ad%25"), "a/b=c:d%");
        assert_eq!(unescape_path_name("50%"), "50%");
    }

    #[test]
    fn dt_utc() {
        // 2023-11-14T22:13:20Z
        assert_eq!(
            DtZone::Utc.date(1_700_000_000_000).to_string(),
            "2023-11-14"
        );
        assert_eq!(DtZone::Utc.date(-1).to_string(), "1969-12-31");
    }
}
