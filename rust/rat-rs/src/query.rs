//! SQL read side, replacing Hive and `rat.Query`. Apache DataFusion (pure
//! Rust) reads the Parquet sinks directly as two Hive-partitioned tables that
//! mirror the DDL in `hive/rat_events.sql`:
//!
//! * `rat_events(event_id, entities, ts_ms, payload, dt, source)`
//! * `correlated(entity, a_id, b_id, ts_ms, dt)`
//!
//! There is no metastore and no `MSCK REPAIR`. Every query lists the
//! directories again, so new partitions show up at once. Only committed
//! `*.parquet` files are visible: staged files are hidden `.tmp` files.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;

use crate::sink::{self, CORRELATED_DIR, RAW_DIR};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Table,
    Jsonl,
}

impl std::str::FromStr for Format {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "table" => Ok(Format::Table),
            "jsonl" | "json" => Ok(Format::Jsonl),
            other => Err(format!("format must be table|jsonl, got {other:?}")),
        }
    }
}

pub async fn context(sink_dir: &Path) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    register(
        &ctx,
        "rat_events",
        &sink_dir.join(RAW_DIR),
        sink::raw_schema(),
        &["dt", "source"],
    )
    .await?;
    register(
        &ctx,
        "correlated",
        &sink_dir.join(CORRELATED_DIR),
        sink::correlated_schema(),
        &["dt"],
    )
    .await?;
    Ok(ctx)
}

async fn register(
    ctx: &SessionContext,
    name: &str,
    dir: &Path,
    schema: arrow_schema::SchemaRef,
    partitions: &[&str],
) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let abs = std::path::absolute(dir)?;
    let url = ListingTableUrl::parse(format!("{}/", abs.display()))?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(
            partitions
                .iter()
                .map(|p| ((*p).to_owned(), DataType::Utf8))
                .collect(),
        );
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table(name, Arc::new(ListingTable::try_new(config)?))?;
    Ok(())
}

pub async fn sql(ctx: &SessionContext, query: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.sql(query).await?.collect().await?)
}

pub fn render(batches: &[RecordBatch], format: Format) -> Result<String> {
    match format {
        Format::Table => Ok(pretty_format_batches(batches)?.to_string()),
        Format::Jsonl => {
            let mut buf = Vec::new();
            {
                let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            Ok(String::from_utf8(buf)?)
        }
    }
}

/// The `rat.Query` report: the 10 latest raw envelopes and the 10 latest pairs.
pub const LEGACY_QUERIES: [(&str, &str); 2] = [
    (
        "== rat_events (raw) ==",
        "SELECT event_id, source, ts_ms, dt FROM rat_events ORDER BY ts_ms DESC LIMIT 10",
    ),
    (
        "== correlated ==",
        "SELECT entity, a_id, b_id, ts_ms, dt FROM correlated ORDER BY ts_ms DESC LIMIT 10",
    ),
];
