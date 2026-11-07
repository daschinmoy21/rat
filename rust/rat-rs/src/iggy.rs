//! Iggy consumer over the HTTP transport.
//!
//! Endpoints used (checked against the iggy server source, `core/server/src/http.rs`):
//! * `POST /users/login` `{username,password}` or
//!   `POST /personal-access-tokens/login` `{token}` -> `{user_id, access_token:{token,expiry}}`
//! * `GET /streams/{stream}` -> `{.., topics:[{id,name,partitions_count,..}]}`
//! * `GET /streams/{stream}/topics/{topic}` -> `{.., partitions:[{id,current_offset,messages_count,..}]}`
//! * `GET /streams/{stream}/topics/{topic}/messages?consumer_id=&partition_id=&kind=offset&value=&count=&auto_commit=false`
//!   -> `{partition_id,current_offset,count,messages:[{header:{offset,..},payload:<base64>,..}]}`
//!
//! Offsets live in the rat-rs checkpoint, committed in the same atomic step
//! as the Parquet output. Server-side consumer offsets are never read or
//! written (`auto_commit=false`), so replay and restart semantics do not
//! depend on the broker.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use regex_lite::Regex;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::json;
use tokio::runtime::Runtime;
use tokio::signal::unix::{Signal, SignalKind, signal};

use crate::checkpoint::Position;
use crate::pipeline::{Fetch, Source};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Starting {
    Earliest,
    Latest,
}

impl std::str::FromStr for Starting {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Starting::Earliest),
            "latest" => Ok(Starting::Latest),
            other => Err(format!(
                "starting offsets must be earliest|latest, got {other:?}"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IggyConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub pat: Option<String>,
    pub stream: String,
    /// Full-match regex for topic names.
    pub topic_pattern: String,
    /// Search regex. Matching topics are skipped (DLQs).
    pub topic_exclude: String,
    pub poll_count: u32,
    pub consumer_id: String,
    pub starting: Starting,
    pub request_timeout: Duration,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct IggyStats {
    pub polls: u64,
    pub messages: u64,
    /// Offsets skipped because the server no longer had them (retention).
    pub offset_gaps: u64,
    pub errors: u64,
}

pub struct IggySource {
    rt: Runtime,
    inner: Inner,
    stop: bool,
    sigint: Signal,
    sigterm: Signal,
}

struct Inner {
    client: Client,
    base: Url,
    cfg: IggyConfig,
    include: Regex,
    exclude: Option<Regex>,
    token: Option<String>,
    offsets: BTreeMap<String, BTreeMap<u32, u64>>,
    first_run: bool,
    stats: IggyStats,
}

#[derive(Deserialize)]
struct Identity {
    access_token: Option<Token>,
}

#[derive(Deserialize)]
struct Token {
    token: String,
}

#[derive(Deserialize)]
struct StreamDetails {
    #[serde(default)]
    topics: Vec<TopicSummary>,
}

#[derive(Deserialize)]
struct TopicSummary {
    name: String,
}

#[derive(Deserialize)]
struct TopicDetails {
    #[serde(default)]
    partitions: Vec<PartitionInfo>,
}

#[derive(Deserialize)]
struct PartitionInfo {
    id: u32,
    current_offset: u64,
    messages_count: u64,
}

#[derive(Deserialize)]
struct Polled {
    #[serde(default)]
    messages: Vec<PolledMessage>,
}

#[derive(Deserialize)]
struct PolledMessage {
    header: PolledHeader,
    payload: String,
}

#[derive(Deserialize)]
struct PolledHeader {
    offset: u64,
}

enum HttpError {
    Unauthorized,
    NotFound,
    Other(anyhow::Error),
}

impl From<reqwest::Error> for HttpError {
    fn from(e: reqwest::Error) -> Self {
        HttpError::Other(e.into())
    }
}

impl IggySource {
    pub fn open(cfg: IggyConfig, position: &Position) -> Result<IggySource> {
        let (offsets, first_run) = match position {
            Position::Start => (BTreeMap::new(), true),
            Position::Iggy { offsets } => (offsets.clone(), false),
            other => bail!("checkpoint position {other:?} is not an Iggy position"),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (sigint, sigterm) = {
            let _guard = rt.enter();
            (
                signal(SignalKind::interrupt())?,
                signal(SignalKind::terminate())?,
            )
        };
        let client = Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .context("build HTTP client")?;
        let base = Url::parse(&cfg.url).with_context(|| format!("RAT_IGGY_URL {:?}", cfg.url))?;
        let include = Regex::new(&format!("^(?:{})$", cfg.topic_pattern))
            .with_context(|| format!("RAT_IGGY_TOPIC_PATTERN {:?}", cfg.topic_pattern))?;
        let exclude = if cfg.topic_exclude.is_empty() {
            None
        } else {
            Some(
                Regex::new(&cfg.topic_exclude)
                    .with_context(|| format!("RAT_IGGY_TOPIC_EXCLUDE {:?}", cfg.topic_exclude))?,
            )
        };
        Ok(IggySource {
            rt,
            inner: Inner {
                client,
                base,
                cfg,
                include,
                exclude,
                token: None,
                offsets,
                first_run,
                stats: IggyStats::default(),
            },
            stop: false,
            sigint,
            sigterm,
        })
    }

    pub fn stats(&self) -> &IggyStats {
        &self.inner.stats
    }

    pub fn topic_selected(&self, name: &str) -> bool {
        self.inner.topic_selected(name)
    }

    fn check_signals(&mut self) {
        let (sigint, sigterm) = (&mut self.sigint, &mut self.sigterm);
        let hit = self.rt.block_on(async {
            tokio::select! {
                biased;
                _ = sigint.recv() => true,
                _ = sigterm.recv() => true,
                _ = std::future::ready(()) => false,
            }
        });
        if hit {
            eprintln!("rat-rs: shutdown requested, finishing after the current batch");
            self.stop = true;
        }
    }
}

impl Inner {
    pub fn topic_selected(&self, name: &str) -> bool {
        self.include.is_match(name) && !self.exclude.as_ref().is_some_and(|re| re.is_match(name))
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("RAT_IGGY_URL cannot be a base URL"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    async fn login(&mut self) -> Result<()> {
        let (url, body) = match &self.cfg.pat {
            Some(pat) => (
                self.url(&["personal-access-tokens", "login"])?,
                json!({"token": pat}),
            ),
            None => (
                self.url(&["users", "login"])?,
                json!({"username": self.cfg.username, "password": self.cfg.password}),
            ),
        };
        let response = self.client.post(url).json(&body).send().await?;
        if !response.status().is_success() {
            bail!(
                "iggy login failed: HTTP {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let identity: Identity = response.json().await.context("decode login response")?;
        self.token = Some(
            identity
                .access_token
                .context("login response carried no access_token")?
                .token,
        );
        Ok(())
    }

    async fn get<T: for<'de> Deserialize<'de>>(
        &self,
        url: Url,
        query: &[(&str, String)],
    ) -> Result<T, HttpError> {
        let token = self.token.as_deref().unwrap_or_default();
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .query(query)
            .send()
            .await?;
        match response.status() {
            StatusCode::UNAUTHORIZED => Err(HttpError::Unauthorized),
            StatusCode::NOT_FOUND => Err(HttpError::NotFound),
            status if !status.is_success() => Err(HttpError::Other(anyhow!(
                "HTTP {status}: {}",
                response.text().await.unwrap_or_default()
            ))),
            _ => Ok(response.json().await?),
        }
    }

    async fn fetch_async(&mut self, max: usize) -> Result<Vec<Vec<u8>>, HttpError> {
        if self.token.is_none() {
            self.login().await.map_err(HttpError::Other)?;
        }
        let stream: StreamDetails = match self
            .get(
                self.url(&["streams", &self.cfg.stream])
                    .map_err(HttpError::Other)?,
                &[],
            )
            .await
        {
            Ok(s) => s,
            Err(HttpError::NotFound) => return Ok(Vec::new()), // stream not created yet
            Err(e) => return Err(e),
        };
        let mut topics: Vec<String> = stream
            .topics
            .into_iter()
            .map(|t| t.name)
            .filter(|n| self.topic_selected(n))
            .collect();
        topics.sort();

        // (topic, partition, next offset, end offset exclusive)
        let mut work: Vec<(String, u32, u64, u64)> = Vec::new();
        for topic in &topics {
            let url = self
                .url(&["streams", &self.cfg.stream, "topics", topic])
                .map_err(HttpError::Other)?;
            let details: TopicDetails = match self.get(url, &[]).await {
                Ok(d) => d,
                Err(HttpError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            let first_run = self.first_run;
            let starting = self.cfg.starting;
            let known = self.offsets.entry(topic.clone()).or_default();
            let mut partitions = details.partitions;
            partitions.sort_by_key(|p| p.id);
            for p in partitions {
                let end = if p.messages_count == 0 {
                    0
                } else {
                    p.current_offset + 1
                };
                // Spark parity: startingOffsets applies only to partitions seen on the
                // first run; partitions discovered later start from the earliest offset.
                let next = *known.entry(p.id).or_insert(match (first_run, starting) {
                    (true, Starting::Latest) => end,
                    _ => 0,
                });
                if next < end {
                    work.push((topic.clone(), p.id, next, end));
                }
            }
        }
        self.first_run = false;

        let mut records = Vec::new();
        for (topic, partition, mut next, end) in work {
            while next < end && records.len() < max {
                let want = (max - records.len())
                    .min(self.cfg.poll_count as usize)
                    .max(1);
                let url = self
                    .url(&["streams", &self.cfg.stream, "topics", &topic, "messages"])
                    .map_err(HttpError::Other)?;
                let query = [
                    ("consumer_id", self.cfg.consumer_id.clone()),
                    ("partition_id", partition.to_string()),
                    ("kind", "offset".to_owned()),
                    ("value", next.to_string()),
                    ("count", want.to_string()),
                    ("auto_commit", "false".to_owned()),
                ];
                self.stats.polls += 1;
                let polled: Polled = match self.get(url, &query).await {
                    Ok(p) => p,
                    Err(HttpError::NotFound) => break,
                    Err(e) => {
                        if records.is_empty() {
                            return Err(e);
                        }
                        // Keep what was read: its offsets are already recorded.
                        if let HttpError::Other(err) = e {
                            eprintln!("rat-rs: iggy poll {topic}/{partition} failed: {err:#}");
                        }
                        self.stats.errors += 1;
                        return Ok(records);
                    }
                };
                if polled.messages.is_empty() {
                    break;
                }
                for message in polled.messages {
                    let offset = message.header.offset;
                    if offset < next {
                        continue;
                    }
                    if offset > next {
                        self.stats.offset_gaps += offset - next;
                    }
                    let payload = STANDARD.decode(message.payload.as_bytes()).map_err(|e| {
                        HttpError::Other(anyhow!("message payload is not base64: {e}"))
                    })?;
                    records.push(payload);
                    next = offset + 1;
                    self.stats.messages += 1;
                    if let Some(parts) = self.offsets.get_mut(&topic) {
                        parts.insert(partition, next);
                    }
                }
            }
            if records.len() >= max {
                break;
            }
        }
        Ok(records)
    }
}

impl Source for IggySource {
    fn is_streaming(&self) -> bool {
        true
    }
    fn position(&self) -> Position {
        Position::Iggy {
            offsets: self.inner.offsets.clone(),
        }
    }

    fn fetch(&mut self, max: usize) -> Result<Fetch> {
        let previous_offsets = self.inner.offsets.clone();
        let previous_first_run = self.inner.first_run;
        for attempt in 0..2 {
            let result = self.rt.block_on(self.inner.fetch_async(max));
            if result.is_err() {
                // An invalid response can arrive after part of a poll has
                // been decoded. No records are handed to the pipeline on
                // error, so none of those offset changes may be committed.
                self.inner.offsets.clone_from(&previous_offsets);
                self.inner.first_run = previous_first_run;
            }
            match result {
                Ok(records) if records.is_empty() => return Ok(Fetch::Idle),
                Ok(records) => return Ok(Fetch::Records(records)),
                Err(HttpError::Unauthorized) if attempt == 0 => self.inner.token = None,
                Err(HttpError::Unauthorized) => {
                    bail!("iggy rejected the credentials after re-login")
                }
                Err(HttpError::NotFound) => return Ok(Fetch::Idle),
                Err(HttpError::Other(e)) => {
                    // Transient broker/network trouble: nothing was consumed, retry after the trigger.
                    self.inner.stats.errors += 1;
                    eprintln!("rat-rs: iggy fetch failed, will retry: {e:#}");
                    return Ok(Fetch::Idle);
                }
            }
        }
        Ok(Fetch::Idle)
    }

    fn should_stop(&mut self) -> bool {
        if !self.stop {
            self.check_signals();
        }
        self.stop
    }

    fn idle(&mut self, interval: Duration) {
        let (sigint, sigterm) = (&mut self.sigint, &mut self.sigterm);
        let hit = self.rt.block_on(async {
            tokio::select! {
                _ = sigint.recv() => true,
                _ = sigterm.recv() => true,
                _ = tokio::time::sleep(interval) => false,
            }
        });
        if hit {
            eprintln!("rat-rs: shutdown requested");
            self.stop = true;
        }
    }
}
