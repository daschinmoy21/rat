# Iggy interface contract (producers <-> broker <-> Rust engine)

This file pins how rat talks to Apache Iggy, so the Python producers
(`producers/rat_producers/iggy.py`) and the Rust consumer engine (reqwest over
HTTP) agree byte for byte. Everything below was checked against the Iggy
source at tag `server-0.9.0` (`core/server/src/http.rs`,
`core/server/src/http/handlers.rs`, `core/common/src/http/messages/*`,
`core/common/src/types/message/*`). The pinned image
`docker.io/apache/iggy:0.9.0` was built from commit `362bd78` on that release
line, and that commit has the same HTTP and serde code.

Iggy is **not** Kafka wire-compatible. Nothing here speaks the Kafka protocol.

## Broker

| Item          | Value                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------- |
| Image         | `docker.io/apache/iggy:0.9.0@sha256:85c9f88ceb41ffa3f9b291372e7aaec9c99fb4fdbd4279b7e2037ad2eb8b884e` |
| Compose       | `infra/iggy/compose.yml` (container `rat-iggy`, volume `iggy-data` at `/app/local_data`)  |
| HTTP          | `http://127.0.0.1:3000` (the only transport rat uses)                                     |
| TCP           | 8090, published on 127.0.0.1 only, unused by rat; QUIC and WebSocket are disabled       |
| Health        | `GET /ping` -> `200`, body `pong` (no auth)                                               |
| Root user     | set from `IGGY_ROOT_USERNAME` / `IGGY_ROOT_PASSWORD` **on first boot only** (default `iggy`/`iggy` in compose) |

The image ships no config file and falls back to the embedded default
`core/server/config.toml`. Its listeners bind 127.0.0.1, so compose overrides
`IGGY_HTTP_ADDRESS=0.0.0.0:3000` (and the other three transports) inside the
container.

## Launch (local, reproducible)

```bash
podman pull docker.io/apache/iggy:0.9.0        # digest pinned in compose
./scripts/iggy-up.sh                            # compose up, wait for /ping, `rat topics`
#   which is the same as:
podman-compose -f infra/iggy/compose.yml up -d  # or: docker compose -f ...
./scripts/iggy-wait.sh                          # polls GET /ping until "pong" (120 s)
RAT_BROKER=iggy uv run rat topics               # idempotent stream + topics + DLQs

RAT_BROKER=iggy uv run rat run                  # producers -> Iggy
RAT_BROKER=iggy uv run rat status               # broker up, cursors, DLQ depths
RAT_BROKER=iggy uv run rat dlq --n 5            # DLQ peek with rat.error
RAT_BROKER=iggy uv run rat dash                 # live view tails events.* from Iggy
RAT_BROKER=iggy uv run python scripts/fixture.py   # smoke envelopes

./scripts/iggy-up.sh --down                     # stop (volume iggy-data kept)
podman volume rm iggy_iggy-data                 # full reset (name = <project>_iggy-data)
```

systemd: `./deploy/install.sh --broker iggy [--start]` installs
`rat-iggy.service` (compose up plus a wait for `/ping`), `rat-producers.service`
(broker-neutral: `make-topics.sh` dispatches on `RAT_BROKER`), and
`rat-engine.service` (`target/release/rat-rs consume`, which must be built
first). Select `--broker kafka` for legacy deployments. New installer
configurations default to Iggy; an existing
explicit broker choice is preserved.

## Client environment (shared by Python and Rust)

| Variable             | Default                 | Meaning                                              |
| -------------------- | ----------------------- | ---------------------------------------------------- |
| `RAT_BROKER`         | `kafka`                 | `iggy` selects this backend in the Python producers  |
| `RAT_IGGY_URL`       | `http://127.0.0.1:3000` | Base URL, no trailing slash                          |
| `RAT_IGGY_USERNAME`  | `iggy`                  | Login user                                           |
| `RAT_IGGY_PASSWORD`  | `iggy`                  | Login password                                       |
| `RAT_IGGY_PAT`       | unset                   | Personal access token. Used instead of user/password when set (Python also accepts `RAT_IGGY_TOKEN`) |
| `RAT_IGGY_STREAM`    | `rat`                   | Stream (name or numeric id) that holds every rat topic |
| `RAT_IGGY_TIMEOUT_S` | `10`                    | Per-request timeout                                  |
| `RAT_PARTITIONS`     | `6`                     | Partitions per topic at provisioning (same as Kafka) |
| `RAT_EVENTS_RETENTION_MS` | `604800000` (7d)   | Event topic `message_expiry` (converted to µs)       |
| `RAT_DLQ_RETENTION_MS`    | `2592000000` (30d) | DLQ topic `message_expiry` (converted to µs)         |

## Auth

```http
POST /users/login
Content-Type: application/json

{"username": "iggy", "password": "iggy"}
```

or, with a PAT:

```http
POST /personal-access-tokens/login
Content-Type: application/json

{"token": "<raw pat>"}
```

200 response (`IdentityInfo`):

```json
{"user_id": 0, "access_token": {"token": "<jwt>", "expiry": 1762430000}}
```

Send `Authorization: Bearer <jwt>` on every other request. Tokens expire after
`http.jwt.access_token_expiry` (1 h by default), and the JWT secret is random
per server start unless it is configured. **Any 401 means log in again once and
retry.** Clients must not cache a token across a broker restart.

## Topic mapping

| Kafka (legacy)             | Iggy                                                |
| -------------------------- | --------------------------------------------------- |
| topic `events.<source>`    | stream `$RAT_IGGY_STREAM` (`rat`), topic `events.<source>` |
| topic `events.<source>.dlq`| stream `rat`, topic `events.<source>.dlq`           |
| record key                 | `partitioning: messages_key` (key = first entity, else `event_id`) |
| record value               | message `payload` (same JSON bytes)                 |
| header `rat.error`         | user header `rat.error` (kind `string`, value truncated to 255 bytes) |
| partition N (0-based)      | partition N (0-based in 0.9.0, ids `0..partitions_count-1`) |
| offset                     | message `header.offset` (u64, per partition, starts at 0, dense) |
| consumer group offsets     | per-partition consumer offsets (`consumer_id`), see below |

Path segments are Iggy identifiers. A segment that parses as `u32` is a
numeric id and anything else is a name, so topics are always addressed by name
(`/streams/rat/topics/events.hn`) and names must never be all digits.

Provisioning is idempotent, from `uv run rat topics` (or
`./scripts/make-topics.sh` with `RAT_BROKER=iggy`). It creates the stream plus
one event topic and one DLQ topic per `plugins/*/plugin.toml`. The broker does
not auto-create topics, so producing to a missing topic is a `404` and the row
goes to the DLQ or the spool.

**Durability is explicit:** every new topic uses `options.durability=persisted`
and `options.consumer_offset_durability=persisted`. Iggy 0.9.0 defaults to
`replicated`, which can acknowledge buffered messages before a stable-storage
barrier. In a real test, all 10,000 such messages were lost on a forced broker
kill before flush. The client therefore requires both HTTP 201 and
`Iggy-Durability: persisted`. Existing topics with any weaker or unknown
policy cause provisioning to fail without modifying their data. Choose a new
stream and provision it before switching existing deployments.

## Provisioning calls

```http
GET  /streams/rat                          -> 200 StreamDetails | 404
POST /streams            {"name": "rat"}   -> 200 StreamDetails (400 name exists)
GET  /streams/rat/topics/events.hn         -> 200 TopicDetails | 404
POST /streams/rat/topics
{
  "name": "events.hn",
  "partitions_count": 6,
  "compression_algorithm": "none",
  "message_expiry": 604800000000,   // microseconds; 0 = server default, 18446744073709551615 = never
  "max_topic_size": 0,              // bytes; 0 = server default, u64::MAX = unlimited
  "options": {"durability": "persisted", "consumer_offset_durability": "persisted"}
}                                          -> 200 TopicDetails (400 name exists)
```

`TopicDetails` fields used by rat: `id`, `name`, `partitions_count`,
`messages_count`, and `partitions[]`, where each partition has `id`,
`current_offset` (the last offset written, 0 when empty), and
`messages_count`. `GET /streams/rat` returns `topics[]` with `name`,
`partitions_count`, and `messages_count`.

## Produce

```http
POST /streams/rat/topics/events.hn/messages
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "partitioning": {"kind": "messages_key", "value": "<base64(key bytes)>"},
  "messages": [
    {
      "id": 0,
      "payload": "<base64(envelope JSON bytes)>",
      "user_headers": [
        {"key":   {"kind": "string", "value": "<base64('rat.error')>"},
         "value": {"kind": "string", "value": "<base64(reason utf-8)>"}}
      ]
    }
  ]
}
```

* All bytes go over the wire as **standard base64 with padding** (RFC 4648 §4,
  not URL-safe). That covers payload, partitioning value, and header key and
  value.
* `partitioning.kind` is one of `balanced` (`value` = `""`),
  `partition_id` (`value` = base64 of u32 LE), or `messages_key` (`value` =
  base64 of 1..255 key bytes). rat always sends `messages_key`, and keys are
  truncated to 255 bytes.
* `id: 0` (or omitted) means the server mints a random u128. Iggy does not
  dedupe on id.
* `user_headers` is omitted when there are none. Header values must be
  1..255 bytes.
* Success is `201 Created`, with header `Iggy-Durability: persisted` and body
  `{"confirmations":[{"stream_id":1,"topic_id":3,"partition_id":2,"base_offset":41}]}`.
  rat requires **201 plus the persisted attestation** as an ack. A bare 201,
  `replicated`, or missing durability header is rejected. `?ack=none` (202)
  is never used.
* The body is capped at `http.max_request_size` (2 MB). rat sends one message
  per request, which matches the old Kafka per-row `send().get()`.

## Poll (Rust engine and `rat dash`/`rat dlq`)

```http
GET /streams/rat/topics/events.hn/messages
      ?consumer_id=rat-engine&partition_id=0&kind=offset&value=0&count=100&auto_commit=false
Authorization: Bearer <jwt>
```

`kind` is one of `offset` (start at `value`), `first`, `last` (the final
`count` messages), `next` (stored offset + 1, or 0 when nothing is stored), or
`timestamp` (`value` in µs). `count` defaults to 10. `partition_id` defaults to
0, so always send it.

200 response (`PolledMessages`):

```json
{
  "partition_id": 0,
  "current_offset": 41,
  "count": 2,
  "messages": [
    {
      "header": {
        "checksum": 123, "id": 3402823669209384634633746074317682114, "offset": 40,
        "timestamp": 1762430000123456, "origin_timestamp": 1762430000120000,
        "user_headers_length": 0, "payload_length": 180, "reserved": 0
      },
      "payload": "<base64>",
      "user_headers": [
        {"key": {"kind": "string", "value": "cmF0LmVycm9y"},
         "value": {"kind": "string", "value": "<base64>"}}
      ]
    }
  ]
}
```

* `header.id` is a **u128 JSON number**. Deserialize it as `u128` (serde_json
  handles that) and never as `f64` or `u64`. The engine can ignore it.
* `timestamp` is the server append time and `origin_timestamp` is the
  client-side time. Both are **microseconds** since the epoch.
* `user_headers` is a list (empty `[]` when absent). It can also be a base64
  **string** when the headers were stored undecodable (for example client-side
  encrypted), so accept both shapes.
* An empty or caught-up poll returns `200` with `messages: []` and `count: 0`.
  `current_offset` is the partition's last offset, or 0 when the partition is
  empty. To tell "empty" from "one message at offset 0", use
  `partitions[].messages_count` from `TopicDetails`.
* A missing stream or topic is `404`. A partition id outside the topic is `400`
  with `code` `partition_not_found`, so take partitions from `TopicDetails`.

The next poll offset is `last_message.header.offset + 1`.

## Consumer offsets (engine checkpoint)

```http
GET /streams/rat/topics/events.hn/consumer-offsets?consumer_id=rat-engine&partition_id=0
  -> 200 {"partition_id":0,"current_offset":41,"stored_offset":39} | 404 (never stored)

PUT /streams/rat/topics/events.hn/consumer-offsets
{"consumer_id": "rat-engine", "partition_id": 0, "offset": 39}
  -> 204 (replicated, durable)

DELETE /streams/rat/topics/events.hn/consumer-offsets/rat-engine?partition_id=0 -> 204
```

`stored_offset` is the **last processed offset (inclusive)**. The resume point
is `stored_offset + 1`, which is what `kind=next` computes.

The Rust engine (`rat-rs consume`, docs/rust-interface.md) keeps offsets in its
own checkpoint next to the sink. That is the stronger choice, because sink and
offset commit together, and then it needs only the poll permission. The
server-side offsets are optional, for example for lag tooling. If they are
used, the loop per (topic, partition) is:

1. `GET consumer-offsets`. On 404, start at 0 (the Kafka `earliest`
   equivalent; to match `RAT_STARTING_OFFSETS=latest`, start at
   `partitions[].current_offset + 1` instead).
2. `GET messages?kind=offset&value=<next>&count=N&auto_commit=false`.
3. Process the batch and commit the sink.
4. Only then `PUT consumer-offsets` with `offset = last.header.offset`.

Never set `auto_commit=true`: it moves the offset at poll time, before the sink
commits, which loses data on a crash. Consumer ids may be names
(`rat-engine`), and the server hashes them. Numeric strings are treated as
numeric ids. Consumer groups exist (`/consumer-groups`), but rat doesn't use
them over HTTP, because HTTP polls use the plain `consumer` kind.

## Errors

Non-2xx bodies look like this:

```json
{"id": 2010, "code": "topic_id_not_found", "reason": "...", "field": "topic_id"}
```

| Status | Meaning for rat                                                          |
| ------ | ------------------------------------------------------------------------ |
| 400    | Caller error (bad name or partition, "already exists" on create). Not retried |
| 401    | Token missing or expired. Log in again once and retry                    |
| 403    | Permission. Not retried                                                  |
| 404    | Stream, topic, or offset missing                                         |
| 413    | Body too large. Not retried; the row goes to the DLQ                     |
| 429/503/504 | Transient (`Retry-After` may be set). Retried                       |
| connection refused / timeout | Broker down: 60 s producer backoff, then spool    |

## Python producer semantics (unchanged from Kafka)

`emit()` and `dlq()` in `producers/rat_producers/producer.py` behave the same
on both backends:

* There are up to 3 send attempts per row. Each one is a synchronous POST that
  must come back 201 with `Iggy-Durability: persisted`.
* Once attempts run out, the row goes to `events.<source>.dlq` with header
  `rat.error`. If the DLQ also fails, it goes to the JSON-lines spool
  `$RAT_DLQ_SPOOL/dlq.jsonl`.
* Schema-invalid rows go straight to the DLQ.
* A "broker unavailable" error (connection refused, timeout, 503) opens a 60 s
  window where rows skip the network and spool immediately.
* The source cursor (`RAT_CURSOR_DB`) advances after `emit()` returns, so the
  row is durably somewhere: acked on the event topic, acked on the DLQ, or
  appended to the local spool. `emit()` never raises. A schema-invalid row
  (sent to the DLQ) does not move the cursor. This matches the Kafka path
  exactly. The Iggy producer reports success only on HTTP 201 with a persisted
  durability attestation.
