# Sparkplug B Export: Configuration and Behaviour

The `sparkplug-export` profile runs this service as a Sparkplug B v3.0 **Edge Node**: each EdgeX
device becomes a Sparkplug Device, its readings are published as metrics, and (optionally) DCMD
messages from a SCADA/Primary Host Application are written back to devices.

## Configuration

Settings live in the `Sparkplug` section of `res/sparkplug-export/configuration.yaml`, which spells
out every key with its default. Every key can be overridden with an environment variable named
after its path (`SPARKPLUG_GROUPID`, `SPARKPLUG_MQTTBROKER_URL`, ...). Durations use Go duration
syntax (`"30s"`), as elsewhere in EdgeX.

| Key | Env override | Default | Notes |
|---|---|---|---|
| `Enabled` | `SPARKPLUG_ENABLED` | `true` | Turns the Sparkplug export off without removing the section. |
| `Namespace` | `SPARKPLUG_NAMESPACE` | `spBv1.0` | |
| `GroupId` | `SPARKPLUG_GROUPID` | `EdgeX` | Required. Must not contain `+`, `/` or `#`. |
| `EdgeNodeId` | `SPARKPLUG_EDGENODEID` | `app-sparkplug-export` | Required. Must not contain `+`, `/` or `#`. |
| `BdSeqStatePath` | `SPARKPLUG_BDSEQSTATEPATH` | `""` | File that persists `bdSeq` across restarts; set it in production. |
| `MqttBroker.Url` | `SPARKPLUG_MQTTBROKER_URL` | `tcp://localhost:1883` | Required. `tcp://`, `ssl://`/`tcps://`, `ws://`, `wss://`. |
| `MqttBroker.ClientIdPrefix` | `SPARKPLUG_MQTTBROKER_CLIENTIDPREFIX` | `edgex-sparkplug` | A random 8-hex-digit suffix is appended, so a restart never collides with the previous session. |
| `MqttBroker.ConnectTimeout` | `SPARKPLUG_MQTTBROKER_CONNECTTIMEOUT` | `30s` | |
| `MqttBroker.KeepAlive` | `SPARKPLUG_MQTTBROKER_KEEPALIVE` | `60s` | |
| `MqttBroker.QoS` | `SPARKPLUG_MQTTBROKER_QOS` | `0` | 0, 1 or 2. |
| `MqttBroker.AutoReconnect` | `SPARKPLUG_MQTTBROKER_AUTORECONNECT` | `true` | Reconnecting republishes NBIRTH and every DBIRTH. With `false`, a broker restart leaves the Edge Node offline until the service restarts. |
| `MqttBroker.Retain` | `SPARKPLUG_MQTTBROKER_RETAIN` | `false` | Sparkplug 3.0 forbids retained BIRTH/DATA/DEATH; a warning is logged if `true`. |
| `MqttBroker.SkipCertVerify` | `SPARKPLUG_MQTTBROKER_SKIPCERTVERIFY` | `false` | |
| `MqttBroker.SecretPath` | `SPARKPLUG_MQTTBROKER_SECRETPATH` | `mqtt` | SecretStore secret with `username`/`password`/`cacert`/`clientcert`/`clientkey`. |
| `MqttBroker.AuthMode` | `SPARKPLUG_MQTTBROKER_AUTHMODE` | `none` | `none`, `usernamepassword`, `cacert`, `clientcert`. |
| `MqttBroker.RetryDuration` | `SPARKPLUG_MQTTBROKER_RETRYDURATION` | `600s` | How long the initial connection is retried. |
| `MqttBroker.RetryInterval` | `SPARKPLUG_MQTTBROKER_RETRYINTERVAL` | `5s` | |
| `Payload.MetricNameFormat` | `SPARKPLUG_PAYLOAD_METRICNAMEFORMAT` | `{metric_level1}/{metric_level2}/{resourceName}` | See [Metric naming](#metric-naming). |
| `DeviceLifecycle.PollInterval` | `SPARKPLUG_DEVICELIFECYCLE_POLLINTERVAL` | `30s` | core-metadata polling for DBIRTH/DDEATH; `"0"` disables. See [Device lifecycle](#device-lifecycle). |
| `Commands.Enabled` | `SPARKPLUG_COMMANDS_ENABLED` | `false` | Allows DCMD writes; see [Commands](#commands-dcmd). |

`SPARKPLUG_*` overrides are applied by this service itself (`main.go`) on every start, with or
without a Configuration Provider: the SDK on its own skips environment overrides for custom
sections loaded straight from the file.

Broker credentials are stored the standard EdgeX v4 way: in `InsecureSecrets` for non-secure mode,
or with `POST /api/v3/secret` on this service (port 59708) with `secretName: "mqtt"` in secure
mode.

## Messages

| Message | When |
|---|---|
| NBIRTH | On every (re)connect and on NCMD `Node Control/Rebirth`; carries `bdSeq` and `Node Control/Rebirth`. |
| NDEATH | As the MQTT Will (non-retained, matching `bdSeq`), and explicitly on graceful shutdown. |
| DBIRTH | When a device is added to core-metadata, on its first reading, and whenever a new metric appears; always declares the device's complete metric set. Replayed for every device after NBIRTH. |
| DDATA | For readings of already-declared metrics, by alias. |
| DDEATH | When a device is deleted from core-metadata. |
| NCMD | `Node Control/Rebirth` is handled. |
| DCMD | Forwarded as EdgeX SET commands when `Commands.Enabled`. |

Not implemented: NDATA and Primary Host STATE awareness.

## Metric naming

`Payload.MetricNameFormat` builds each metric name: every `{key}` is replaced by the value of the
tag `key`, so with the default format a resource tagged `metric_level1: Building4`,
`metric_level2: Zone2` in its device profile produces the metric `Building4/Zone2/temperature`.

Lookup order for each `{key}`: the reading's tags (EdgeX copies device-resource tags onto every
reading), then the event's tags (device tags), then the built-ins `resourceName`, `deviceName`,
`profileName` and `sourceName`. An unresolved placeholder becomes empty and empty `/` segments are
dropped, so an untagged resource is simply named `temperature` rather than `//temperature`. A
format that resolves to nothing at all falls back to the resource name.

A device born from core-metadata gets its metric names from the same tags (device-resource tags,
then device tags), so its DBIRTH declares exactly the names its readings will later use.
`{sourceName}` resolves to the resource name there; avoid it in the format if devices use
multi-resource commands.

## Device lifecycle

EdgeX core-metadata has no device-change feed this service can subscribe to without changing its
trigger, so it polls core-metadata every `DeviceLifecycle.PollInterval` (default 30 s), which
bounds how late a DBIRTH/DDEATH can be:

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
was born from, through core-command. The new value reaches the host as normal DDATA once the
device reports it.

A metric is skipped (with a warning) if it is unknown for the device, has no value, carries a
value that doesn't match its birth data type, is `Bytes`, or targets a resource whose profile
`ReadWrite` isn't `W`, `RW` or `WR`.

This is **off by default**: it lets any client able to publish on the broker drive equipment, so
enable it deliberately and secure the broker's ACLs accordingly. It needs the `core-command` and
`core-metadata` clients from common config; without them it stays disabled with a warning.

## Specification version

This service follows Sparkplug 3.0. The topic structure, payload encoding and message types it uses
are the same as in 2.2; 3.0 mainly clarifies rules (non-retained messages, `bdSeq` handling, NDEATH
on disconnect) that this service follows, so hosts written for 2.2 consume it unchanged. The
compliance test in `test/tahu-compliance/` checks this against Eclipse Tahu's reference host.
