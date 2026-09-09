# Receiving-Side Database Architecture for Sparkplug B Data

## 1. Scope and audience

This document is for whoever builds the **consumer** of the Sparkplug B v3.0 MQTT stream this
service (`app-service-sparkplug`, configured with the `sparkplug-export` profile) produces. It is
not shipped code in this repo — there is no consumer service here. It describes how to structure
the databases that receive this stream: what to write, where, and why, using InfluxDB for
short-retention "hot" data and BigQuery for long-retention "cold" data.

If you haven't read how the producer side works, the short version: one running instance of this
service is one Sparkplug **Edge Node** (`spBv1.0/{GroupId}/.../{EdgeNodeId}[/{Device}]`); each
distinct EdgeX device name becomes its own Sparkplug **Device**, auto-birthed on first sighting.
See `internal/sparkplug/node.go` for the full lifecycle (NBIRTH/NDEATH with `bdSeq` and an MQTT
Will, per-device DBIRTH/DDATA, sequence numbers, and NCMD `Node Control/Rebirth` handling).

## 2. Consumer responsibilities recap

Before any of this data reaches a database, the consumer has to do a few things correctly — get
these wrong and the two stores below will silently disagree with reality:

- **Decode protobuf** using the same vendored `internal/sparkplug/spplugb/sparkplug_b.proto` this
  producer uses (or the canonical schema from `eclipse-tahu/tahu`) — don't hand-roll a parser.
- **Maintain an alias → metric-name cache**, keyed by `(group_id, edge_node_id, device_id)` and
  populated from BIRTH messages. DATA messages carry only the alias, not the name — a DDATA that
  references an alias the consumer hasn't seen a BIRTH for is not writable to either store until
  resolved, because you don't yet know what it is.
- **Track `seq` per Edge Node** to detect gaps or reordering, and **`bd_seq`** (carried in every
  NBIRTH/NDEATH) to detect a genuinely new Edge Node session. On a detected gap, publish
  `Node Control/Rebirth = true` to that Edge Node's NCMD topic
  (`spBv1.0/{group}/NCMD/{edgeNode}`) — this is exactly the command this producer's `handleNCmd`
  already implements, so the two sides close the loop: the consumer asks, the producer replays a
  fresh NBIRTH + DBIRTH for every device it knows about.

## 3. Why two stores

| | InfluxDB (hot) | BigQuery (cold) |
|---|---|---|
| Retention | Short (days–months) | Long (years, effectively indefinite) |
| Write pattern | High-frequency point writes, time-series native | Cheap batched appends, poor fit for per-point streaming writes |
| Query pattern | "what's happening now / recently" — dashboards, alerting | Historical/analytical — BI, audits, ML training sets |
| Cost driver | Memory (series cardinality) | Storage + query bytes scanned |

Neither store is a good fit for the other's job: InfluxDB gets expensive and slow if you ask it to
hold years of full-resolution data across many devices; BigQuery is a poor fit for sub-second
point writes and dashboards that need to feel instant.

## 4. Ingestion pattern: dual-write, not migrate

**Recommendation: write every decoded, name-resolved metric to both stores at ingestion time
(fan-out), rather than writing only to InfluxDB and later ETL'ing aged-out data into BigQuery.**

A migrate-later pipeline (read from Influx once it ages out, transform, write to BigQuery) is a
second stateful system with its own failure modes — a job that needs to track a watermark, retry
partial failures, and never double-migrate. Dual-write is simpler: the consumer writes to both on
every message, and InfluxDB's retention policy just **drops** old data outright once it expires,
because BigQuery already holds the durable copy. If the BigQuery write fails, retry it
independently — it doesn't block the (latency-sensitive) InfluxDB write, and vice versa.

```
                    ┌──────────────┐
 MQTT (spBv1.0/#) → │   Consumer   │ → InfluxDB  (hot, e.g. 30–90 days)
                    │ (decode,     │
                    │  resolve     │ → BigQuery  (cold, indefinite)
                    │  alias→name) │
                    └──────────────┘
```

## 5. InfluxDB (hot) schema

- **Measurement**: a single measurement, e.g. `sparkplug_metrics`, rather than one measurement per
  metric — keeps queries and retention policy management simple.
- **Tags** (indexed, low-cardinality, used for filtering): `group_id`, `edge_node_id`, `device_id`
  (omitted/empty for node-level metrics like `bdSeq`), and — per the metadata model in §7 — a
  couple of curated low-cardinality dimensions like `site_id` and `data_origin`. Do **not** tag
  with the metric name itself if you have hundreds of distinct metrics per device; use a field
  instead (see below), or cardinality explodes.
- **Fields**: one field per metric name, holding its value. Sparkplug's DataTypes don't map 1:1
  onto InfluxDB's line-protocol types (`float64`, `int64`, `uint64`, `bool`, `string`) — collapse
  as follows:

  | Sparkplug DataType | InfluxDB field type |
  |---|---|
  | `Int8`/`Int16`/`Int32`/`Int64` | `int64` |
  | `UInt8`/`UInt16`/`UInt32`/`UInt64` | `uint64` |
  | `Float`/`Double` | `float64` |
  | `Boolean` | `bool` |
  | `String` | `string` |
  | `Bytes`/`DataSet` | `string` (base64/JSON-encoded) — not a good fit for hot storage; consider skipping these in the hot path entirely and relying on BigQuery for them |

- **Retention**: a bucket (InfluxDB 2.x) or retention policy (1.x) of roughly 30–90 days of full
  resolution, environment-dependent. Optionally add a continuous/downsampling task that rolls the
  bucket up into a coarser, longer-lived second bucket (e.g. 5-minute averages retained for a
  year) for slightly-longer-than-hot trend queries without paying BigQuery's query latency.
- One bucket per environment (dev/staging/prod), not per Edge Node — Edge Node/Device stay as tags
  within a shared bucket.

## 6. BigQuery (cold) schema

Two fact tables, provisioned via the schema files and script in `docs/bigquery/` (§9 links these
directly — this isn't just prose, run `docs/bigquery/provision.sh` to create them):

### `sparkplug_metrics` — the fact table

A single **wide table** with one nullable column per Sparkplug value family, rather than a
separate table per data type. This is simpler to query (`WHERE metric_name = 'x'` works the same
regardless of type) and simpler to write (the consumer always writes to the same table). The
trade-off against per-type tables is some wasted columnar storage from unused nullable columns per
row — negligible at Sparkplug's typical row width, and outweighed by not needing N tables and N
write paths.

Columns (see `docs/bigquery/schema/sparkplug_metrics.json` for the exact `bq`-loadable schema):
`timestamp`, `group_id`, `edge_node_id`, `device_id` (nullable), `metric_name`, `alias`,
`data_type`, `value_double`/`value_int64`/`value_bool`/`value_string`/`value_bytes` (exactly one
populated per row), `seq`, `bd_seq`, `ingestion_time`.

**Partitioned by `DATE(timestamp)`, clustered by `(group_id, edge_node_id, device_id,
metric_name)`** — this is the filter shape almost every query uses ("this device's this metric,
over this date range"), so clustering on it directly controls how many bytes BigQuery scans.

### `sparkplug_sessions` — the companion audit table

One row per `(group_id, edge_node_id, bd_seq)`: `birth_time`, `death_time` (nullable — null means
still alive as far as the consumer knows), `death_reason` (`"NDEATH"`, `"will"`, `"gap-detected"`,
etc.). This pairs naturally with the `bdSeq`/NBIRTH/NDEATH lifecycle this producer already emits —
it's a cheap, high-value table for uptime/session auditing that the raw metrics table alone
doesn't give you (you'd have to scan for `bdSeq` metric rows and reconstruct sessions by hand
otherwise).

### Write path

Use the **Storage Write API**, not legacy per-row streaming inserts — buffer writes for a short
window (a few seconds, or a few hundred rows) on the consumer side and batch-commit. Streaming
inserts bill per row regardless of size and get expensive fast at Sparkplug's typical message
rates; batched Storage Write API writes are both cheaper and higher-throughput.

## 7. Metadata model: geography, data origin, and tag normalization

Sparkplug's own payload only carries a metric's *name* (or alias) and *value* — there's no room in
the wire format for "this sensor is at Building 4, is a physical measurement, and its raw PLC tag
was `AI_014` before it got normalized to `zone_4.temperature`". That's real, necessary context,
but it changes rarely (metadata) versus the metric stream that changes constantly (facts). The
standard answer is a small **dimensional model**: keep it in slowly-changing dimension tables,
joined at query time, not duplicated onto every point.

### `dim_metric` — one row per `(group_id, edge_node_id, device_id, metric_name)`, versioned

| Column | Meaning |
|---|---|
| `normalized_tag` | Canonical name, e.g. `zone_4.temperature` |
| `field_tag` | The raw source-system tag/point name, e.g. `AI_014` — kept for traceability back to the PLC/device config |
| `data_origin` | `sensor` (direct physical measurement) / `aggregate` (computed rollup of other metrics) / `virtual` (calculated/derived point with no direct sensor) / `manual` (human-entered value) |
| `unit_of_measure` | e.g. `°C`, `kPa` |
| `description` | Free text |
| `asset_id` | FK into `dim_asset` |
| `effective_from` / `effective_to` | Nullable — standard slowly-changing-dimension (SCD Type 2) versioning, so a re-mapping or unit change doesn't lose history |

### `dim_asset` — one row per physical/logical asset (typically Device or Edge Node level)

| Column | Meaning |
|---|---|
| `asset_id` | Primary key |
| `site_id` / `site_name` | |
| `latitude` / `longitude` | |
| `building` / `floor` / `area` | |
| `parent_asset_id` | Self-referencing FK — builds an asset hierarchy, e.g. site → building → line → device |
| `timezone` | |

Schema files: `docs/bigquery/schema/dim_metric.json`, `docs/bigquery/schema/dim_asset.json` (also
provisioned by `docs/bigquery/provision.sh`).

### Where this metadata comes from

EdgeX's own core-metadata already holds a good chunk of it: each `Device`'s `Location` and
`Labels` fields, and each `DeviceProfile`'s `DeviceResource.Properties.Units` /
`.Description`. **Recommended approach**: a small, separate, periodic sync (e.g. hourly) that
reads EdgeX's core-metadata REST API and upserts into `dim_metric`/`dim_asset` — fully decoupled
from the MQTT hot path, so it needs no changes to this producer.

**Future enhancement, not built now** (consistent with the DDEATH-on-removal and DCMD deferrals
in the producer): Sparkplug's protobuf schema already has a `Metric.properties` field meant for
exactly this kind of attached metadata. A later version of this producer could publish
unit/description/origin directly in each DBIRTH instead of relying on a side-channel sync, letting
the metadata travel with the birth certificate itself. Worth doing eventually; not required for a
working receiving-side architecture today.

### Where it's stored, and how it's queried — not duplicated everywhere

- **BigQuery is the home for both dimension tables** — slowly-changing, low volume, needs
  joins/versioning, exactly BigQuery's strength. A fully-enriched historical query looks like:

  ```sql
  SELECT m.timestamp, m.metric_name, dm.normalized_tag, dm.data_origin, da.site_name,
         COALESCE(m.value_double, m.value_int64, IF(m.value_bool, 1, 0)) AS value
  FROM sparkplug_metrics m
  JOIN dim_metric dm
    ON dm.group_id = m.group_id AND dm.edge_node_id = m.edge_node_id
   AND dm.device_id = m.device_id AND dm.metric_name = m.metric_name
   AND m.timestamp >= dm.effective_from
   AND (dm.effective_to IS NULL OR m.timestamp < dm.effective_to)
  JOIN dim_asset da ON da.asset_id = dm.asset_id
  WHERE DATE(m.timestamp) = CURRENT_DATE();
  ```

- **InfluxDB does not get the rich geographic detail** (lat/long, description) as tags —
  unbounded/high-cardinality tag values blow up Influx's in-memory series index. Only a couple of
  genuinely low-cardinality, frequently-filtered dimensions (`site_id`, `data_origin`) are worth
  writing as Influx tags, populated by the consumer at ingest time from a small in-process cache
  refreshed periodically from `dim_metric`/`dim_asset`. Everything else (lat/long, description,
  the full asset hierarchy) stays BigQuery-only and gets joined in at query/dashboard time — e.g.
  Grafana's mixed-datasource support, or a scheduled BigQuery view feeding a reporting tool.

## 8. Handling out-of-order/duplicate delivery

Use the `seq`/`bd_seq` tracking from §2 *before* either store, not after:

- A DDATA/DBIRTH whose `seq` isn't the expected next value for that Edge Node indicates a gap —
  request a rebirth (§2) and either buffer or drop the out-of-order message depending on how
  strict you need to be; don't write it to either store as if it were in-order, since downstream
  consumers of the fact table assume `seq` is monotonic per session.
- A message whose `bd_seq` doesn't match the currently-tracked session for that Edge Node means a
  new session started (deliberate restart, or the consumer missed the transition) — close out the
  previous row in `sparkplug_sessions` (if not already closed by an NDEATH) and open a new one.
- Duplicate delivery (same `seq`/`bd_seq` twice, e.g. after an at-least-once redelivery) is safe to
  simply re-write to both stores: BigQuery inserts are naturally idempotent-ish for analytics
  purposes at this volume, and InfluxDB's writes are idempotent by point (same series + timestamp
  overwrites, doesn't duplicate).

## 9. Optional curation layer

Keep the raw `sparkplug_metrics` table cheap and long-retained by not querying it directly for
dashboards. Add scheduled BigQuery SQL (or dbt models) that roll it up into hourly/daily
aggregates per `(device_id, metric_name)` — min/max/avg/last — into separate, much smaller
"curated" tables that BI tools query instead. This keeps the expensive full-fidelity scans rare
(ad hoc investigation, ML training) while everyday dashboards stay fast and cheap.

## 10. Operational notes

- **Naming conventions the consumer should expect**: `GroupId`/`EdgeNodeId` are operator-chosen at
  producer configuration time (see `res/sparkplug-export/configuration.yaml`) — the consumer
  should not hardcode assumptions about their format, only about the topic structure
  (`spBv1.0/{group}/{msgType}/{edgeNode}[/{device}]`).
- **Producer-side signals worth alerting on from the consumer's own monitoring, not just the
  producer's**: this producer registers three `go-metrics` counters —
  `SparkplugPublishErrors`, `SparkplugRebirths`, `SparkplugMessagesPublished` — visible through
  EdgeX's standard service metrics. A rising `SparkplugRebirths` count, seen from the consumer
  side as repeated fresh NBIRTH/DBIRTH sequences for the same Edge Node, usually means the
  consumer itself is the one requesting rebirths (a gap-detection false positive, or a genuinely
  flaky link) — worth correlating both sides' metrics rather than debugging either in isolation.
- **Expected volumes**: Sparkplug's own overhead (aliasing, omitting names from DATA messages) is
  designed for this, but per-device/metric cardinality still drives both InfluxDB's memory
  footprint and BigQuery's clustering effectiveness — size the hot bucket's retention window
  around actual measured cardinality, not a guess, before committing to a retention policy.
