//! An order-independent digest of everything committed in a sink directory.
//! It reads only `part-*.parquet` files and ignores hidden/tmp files and
//! Spark's `_spark_metadata`. It works on legacy Spark output too, so
//! `rat-rs digest --sink-dir <spark sink>` gives a direct parity check.
//! (For Spark output this assumes no uncommitted files were left behind by a
//! failed task. Spark's file sink tracks committed files in
//! `_spark_metadata`, which this reader does not consult.)

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result, bail};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use serde_json::{Value, json};

use crate::sink::{self, CORRELATED_DIR, RAW_DIR, unescape_path_name};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Digest {
    pub raw_rows: u64,
    pub correlated_rows: u64,
    /// Correlated rows that are exact duplicates of another row.
    pub correlated_duplicates: u64,
    pub files: u64,
    pub digest: String,
}

/// Canonical rows of both tables, sorted. Each row is a JSON array:
/// `["raw", dt, source, event_id, entities, ts_ms, payload]` or
/// `["correlated", dt, entity, a_id, b_id, ts_ms]`.
pub fn canonical_rows(sink_dir: &Path) -> Result<(Vec<String>, Vec<String>, u64)> {
    let mut raw = Vec::new();
    let mut corr = Vec::new();
    let mut files = 0;
    for (table, out) in [(RAW_DIR, &mut raw), (CORRELATED_DIR, &mut corr)] {
        let root = sink_dir.join(table);
        for path in sink::walk_files(&root, &is_data_file)? {
            files += 1;
            let parts = partition_values(&root, &path);
            read_file(&path, table == RAW_DIR, &parts, out)
                .with_context(|| format!("read {}", path.display()))?;
        }
    }
    raw.sort_unstable();
    corr.sort_unstable();
    Ok((raw, corr, files))
}

pub fn compute(sink_dir: &Path) -> Result<Digest> {
    let (raw, corr, files) = canonical_rows(sink_dir)?;
    let mut hash = Fnv::new();
    for row in raw.iter().chain(corr.iter()) {
        hash.write(row.as_bytes());
        hash.write(b"\n");
    }
    let duplicates = corr.windows(2).filter(|w| w[0] == w[1]).count() as u64;
    Ok(Digest {
        raw_rows: raw.len() as u64,
        correlated_rows: corr.len() as u64,
        correlated_duplicates: duplicates,
        files,
        digest: format!("{:016x}", hash.0),
    })
}

fn is_data_file(path: &Path) -> bool {
    let in_metadata = path
        .components()
        .any(|c| c.as_os_str().to_str().is_some_and(|s| s.starts_with('_')));
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    !in_metadata && name.starts_with("part-") && name.ends_with(".parquet")
}

fn partition_values(root: &Path, file: &Path) -> Vec<(String, String)> {
    let rel = file.strip_prefix(root).unwrap_or(file);
    rel.parent()
        .into_iter()
        .flat_map(|p| p.components())
        .filter_map(|c| {
            let s = c.as_os_str().to_str()?;
            let (k, v) = s.split_once('=')?;
            Some((k.to_owned(), unescape_path_name(v)))
        })
        .collect()
}

fn part(parts: &[(String, String)], key: &str) -> Value {
    parts
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| *v != sink::DEFAULT_PARTITION)
        .map_or(Value::Null, |v| Value::String(v.to_owned()))
}

fn read_file(
    path: &Path,
    is_raw: bool,
    parts: &[(String, String)],
    out: &mut Vec<String>,
) -> Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
    for batch in reader {
        let batch = batch?;
        let col = |name: &str| -> Result<ArrayRef> {
            batch
                .column_by_name(name)
                .cloned()
                .with_context(|| format!("column {name} missing"))
        };
        if is_raw {
            let (event_id, entities, ts, payload) = (
                col("event_id")?,
                col("entities")?,
                col("ts_ms")?,
                col("payload")?,
            );
            for i in 0..batch.num_rows() {
                out.push(
                    json!([
                        "raw",
                        part(parts, "dt"),
                        part(parts, "source"),
                        string_at(&event_id, i)?,
                        list_at(&entities, i)?,
                        int_at(&ts, i)?,
                        string_at(&payload, i)?,
                    ])
                    .to_string(),
                );
            }
        } else {
            let (entity, a, b, ts) = (col("entity")?, col("a_id")?, col("b_id")?, col("ts_ms")?);
            for i in 0..batch.num_rows() {
                out.push(
                    json!([
                        "correlated",
                        part(parts, "dt"),
                        string_at(&entity, i)?,
                        string_at(&a, i)?,
                        string_at(&b, i)?,
                        int_at(&ts, i)?,
                    ])
                    .to_string(),
                );
            }
        }
    }
    Ok(())
}

fn string_at(array: &ArrayRef, i: usize) -> Result<Value> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    let s = match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(i),
        DataType::LargeUtf8 => array.as_string::<i64>().value(i),
        DataType::Utf8View => array.as_string_view().value(i),
        DataType::Binary => std::str::from_utf8(array.as_binary::<i32>().value(i))?,
        other => bail!("expected a string column, got {other}"),
    };
    Ok(Value::String(s.to_owned()))
}

fn int_at(array: &ArrayRef, i: usize) -> Result<Value> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    match array.data_type() {
        DataType::Int64 => Ok(json!(array.as_primitive::<Int64Type>().value(i))),
        other => bail!("expected an int64 column, got {other}"),
    }
}

fn list_at(array: &ArrayRef, i: usize) -> Result<Value> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    let values = match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().value(i),
        DataType::LargeList(_) => array.as_list::<i64>().value(i),
        other => bail!("expected a list column, got {other}"),
    };
    (0..values.len())
        .map(|j| string_at(&values, j))
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

/// FNV-1a 64: stable across platforms and releases, no extra dependency.
struct Fnv(u64);

impl Fnv {
    fn new() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}
