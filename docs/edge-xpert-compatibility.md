# Compatibility with IOTech Edge Xpert's Sparkplug Service

This service (`sparkplug-export` profile) is designed to be a drop-in, open-source counterpart to
the Sparkplug service in IOTech Edge Xpert 2.3
([reference](https://docs.iotechsys.com/edge-xpert23/supporting-services/sparkplug/sparkplug.html)):
same configuration keys, same environment variable overrides, same metric naming, and the same
device lifecycle and command behaviour as seen by a SCADA/Primary Host Application. This page maps
one onto the other and lists every deliberate difference.

## Configuration

Both services read a `Sparkplug` section with the same nesting, so an Edge Xpert configuration (or
its `SPARKPLUG_*` environment overrides) carries over unchanged. All durations use Go duration
syntax (`"30s"`), as elsewhere in EdgeX.

| Key | Env override | Default | Notes |
|---|---|---|---|
| `Namespace` | `SPARKPLUG_NAMESPACE` | `spBv1.0` | |
| `GroupId` | `SPARKPLUG_GROUPID` | `EdgeX` | Required. Must not contain `+`, `/` or `#`. |
| `EdgeNodeId` | `SPARKPLUG_EDGENODEID` | `app-sparkplug-export` | Required. Must not contain `+`, `/` or `#`. |
| `MqttBroker.Url` | `SPARKPLUG_MQTTBROKER_URL` | `tcp://localhost:1883` | Required. `tcp://`, `ssl://`/`tcps://`, `ws://`, `wss://`. |
| `MqttBroker.ClientIdPrefix` | `SPARKPLUG_MQTTBROKER_CLIENTIDPREFIX` | `edgex-sparkplug` | A random 8-hex-digit suffix is appended, as in Edge Xpert. |
| `MqttBroker.ConnectTimeout` | `SPARKPLUG_MQTTBROKER_CONNECTTIMEOUT` | `30s` | |
| `MqttBroker.KeepAlive` | `SPARKPLUG_MQTTBROKER_KEEPALIVE` | `60s` | |
| `MqttBroker.QoS` | `SPARKPLUG_MQTTBROKER_QOS` | `0` | 0, 1 or 2. |
| `MqttBroker.AutoReconnect` | `SPARKPLUG_MQTTBROKER_AUTORECONNECT` | **`true`** | Edge Xpert defaults to `false`; see below. |
| `MqttBroker.Retain` | `SPARKPLUG_MQTTBROKER_RETAIN` | `false` | Sparkplug 3.0 forbids retained BIRTH/DATA/DEATH; a warning is logged if `true`. |
| `MqttBroker.SkipCertVerify` | `SPARKPLUG_MQTTBROKER_SKIPCERTVERIFY` | `false` | |
| `MqttBroker.SecretPath` | `SPARKPLUG_MQTTBROKER_SECRETPATH` | `mqtt` | SecretStore secret with `username`/`password`/`cacert`/`clientcert`/`clientkey`. |
| `MqttBroker.AuthMode` | `SPARKPLUG_MQTTBROKER_AUTHMODE` | `none` | `none`, `usernamepassword`, `cacert`, `clientcert`. |
| `MqttBroker.RetryDuration` | `SPARKPLUG_MQTTBROKER_RETRYDURATION` | `600s` | How long the initial connection is retried. |
| `MqttBroker.RetryInterval` | `SPARKPLUG_MQTTBROKER_RETRYINTERVAL` | `5s` | |
| `Payload.MetricNameFormat` | `SPARKPLUG_PAYLOAD_METRICNAMEFORMAT` | `{metric_level1}/{metric_level2}/{resourceName}` | See [Metric naming](#metric-naming). |

Keys specific to this service, all optional:

| Key | Env override | Default | Purpose |
|---|---|---|---|
| `Enabled` | `SPARKPLUG_ENABLED` | `true` | Turns the Sparkplug export off without removing the section. |
| `BdSeqStatePath` | `SPARKPLUG_BDSEQSTATEPATH` | `""` | File that persists `bdSeq` across restarts. |
| `DeviceLifecycle.PollInterval` | `SPARKPLUG_DEVICELIFECYCLE_POLLINTERVAL` | `30s` | core-metadata polling for DBIRTH/DDEATH; `"0"` disables. Replaces Edge Xpert's `DeviceChangesNotifications`. |
| `Commands.Enabled` | `SPARKPLUG_COMMANDS_ENABLED` | `false` | Allows DCMD writes; see [Commands](#commands-dcmd). |

`SPARKPLUG_*` overrides are applied by this service itself (`main.go`) on every start, with or
without a Configuration Provider: the SDK on its own skips environment overrides for custom
sections loaded straight from the file. `res/sparkplug-export/configuration.yaml` still spells out
every key, including those left at their default, so it doubles as the reference.

Broker credentials are stored the standard EdgeX v4 way: in `InsecureSecrets` for non-secure mode,
or with `POST /api/v3/secret` on this service (port 59708) with `secretName: "mqtt"` in secure
mode. Edge Xpert's documentation shows the equivalent v2 call.

## Behaviour

| Behaviour | Edge Xpert 2.3 | This service |
|---|---|---|
| NBIRTH on connect, with `bdSeq` and `Node Control/Rebirth` | ✓ | ✓ |
| NDEATH as MQTT Will (non-retained, matching `bdSeq`) | ✓ | ✓ |
| NDEATH on graceful shutdown | — (Will only) | ✓ (Sparkplug 3.0 behaviour) |
| NCMD `Node Control/Rebirth` → NBIRTH + every DBIRTH | — | ✓ |
| DBIRTH when a device is added to core-metadata | ✓ | ✓, by polling (declares every visible profile resource with `is_null`) |
| DBIRTH on a device's first reading | — | ✓ (so data flows without core-metadata too) |
| DBIRTH re-sent when a new metric appears | — | ✓ (always declares the device's complete metric set) |
| DDATA for readings, by alias | ✓ | ✓ |
| DDEATH when a device is deleted from core-metadata | ✓ | ✓, by polling |
| DCMD → EdgeX SET command on a writable resource | ✓ | ✓, opt-in |
| NDATA | — | — |
| Primary Host STATE awareness | — | — |
| Sparkplug specification | 2.2 | 3.0 |

## Metric naming

`Payload.MetricNameFormat` behaves as in Edge Xpert: each `{key}` is replaced by the value of the
tag `key`, so with the default format a resource tagged `metric_level1: Building4`,
`metric_level2: Zone2` in its device profile produces the metric `Building4/Zone2/temperature`.

Lookup order for each `{key}`: the reading's tags (EdgeX copies device-resource tags onto every
reading), then the event's tags (device tags), then the built-ins `resourceName`, `deviceName`,
`profileName` and `sourceName`. Edge Xpert doesn't define what happens when a tag is missing; here
an unresolved placeholder becomes empty and empty `/` segments are dropped, so an untagged
resource is simply named `temperature` rather than `//temperature`. A format that resolves to
nothing at all falls back to the resource name.

A device born from core-metadata gets its metric names from the same tags (device-resource tags,
then device tags), so its DBIRTH declares exactly the names its readings will later use.
`{sourceName}` resolves to the resource name there; avoid it in the format if devices use
multi-resource commands.

## Device lifecycle

Edge Xpert learns about device changes through a proprietary `DeviceChangesNotifications`
channel. EdgeX's open-source core-metadata has no equivalent this service can subscribe to without
changing its trigger, so this service polls core-metadata instead, every
`DeviceLifecycle.PollInterval` (default 30 s), which bounds how late a DBIRTH/DDEATH can be:

- A device present in core-metadata but not yet born gets a DBIRTH declaring every non-hidden
  resource of a supported value type (all but Object/Array), with `is_null = true` until readings
  arrive as DDATA.
- A device that was seen in core-metadata and has since been deleted gets a DDEATH (timestamp and
  `seq` only) and is forgotten, so re-adding it produces a fresh DBIRTH.
- If core-metadata can't be reached, nothing changes: an outage never looks like a mass deletion.

Polling needs the `core-metadata` entry under `Clients`, which common config supplies in a
standard EdgeX deployment. Without it, polling is disabled with a warning and devices are born
from their first reading only (no DDEATH).

## Commands (DCMD)

With `Commands.Enabled: true`, this service subscribes to
`{Namespace}/{GroupId}/DCMD/{EdgeNodeId}/+`. Each metric of a DCMD, addressed by name or by the
alias from the device's DBIRTH, is translated into an EdgeX SET command on the device resource it
was born from, through core-command — the same thing Edge Xpert does, and the success log line
reads `successfully issue SET command`. The new value reaches the host as normal DDATA once the
device reports it.

A metric is skipped (with a warning) if it is unknown for the device, has no value, carries a
value that doesn't match its birth data type, is `Bytes`, or targets a resource whose profile
`ReadWrite` isn't `W`, `RW` or `WR`.

This is **off by default**, unlike Edge Xpert: it lets any client able to publish on the broker
drive equipment, so enable it deliberately and secure the broker's ACLs accordingly. It needs the
`core-command` and `core-metadata` clients from common config; without them it stays disabled with
a warning.

## Why the differences

- **`AutoReconnect: true`**: with Edge Xpert's `false`, a broker restart leaves the Edge Node
  offline until the service is restarted. Reconnecting always republishes NBIRTH and every DBIRTH.
- **Sparkplug 3.0 vs 2.2**: the topic structure, payload encoding and message types used here are
  the same in both versions; 3.0 mainly clarifies rules (non-retained messages, `bdSeq` handling,
  NDEATH on disconnect) that this service follows. Hosts written for 2.2 consume it unchanged. The
  compliance test in `test/tahu-compliance/` checks this against Eclipse Tahu's reference host.
- **Service port**: this service listens on 59708 (`Service.Port`), not Edge Xpert's port.
