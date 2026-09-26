# Sparkplug B Compliance Testing Against Eclipse Tahu's Reference Host Application

## What this validates

`internal/sparkplug/node.go`'s unit tests (`node_test.go`) check our own understanding of the
Sparkplug B spec against hand-written fakes — they can't catch a case where our understanding of
the spec itself is wrong. `test/tahu-compliance/` closes that gap by running our actual Edge Node
against **`tahu-host-compat`**, Eclipse Tahu's reference, spec-compliant Sparkplug B Host
Application (from `eclipse-tahu/tahu`'s `java/` module), and checking that it accepts our
NBIRTH/DBIRTH/DDATA cleanly.

If `tahu-host-compat` ever requests a rebirth in its logs during this test, that's it detecting a
sequencing or structural problem with a message we sent — a real, independent signal our own unit
tests cannot produce, since they only check against our own understanding, not someone else's
implementation of the same spec.

## Running it

```sh
make compliance-test
```

or directly:

```sh
cd test/tahu-compliance
./run-compliance-test.sh
```

Requires Docker and Compose v2. Nothing is exposed on the host — every check runs via
`docker compose exec`/`logs`/a throwaway container joined to the stack's network namespace — so
this is safe to run in CI without any host port allocation. On exit (success or failure), the full
container logs are written to `test/tahu-compliance/compliance-test.log` and the stack is torn
down.

## What the script checks, and why

0. **`tahu-host-compat` starts before `sparkplug-export`, deliberately, not just by `depends_on`
   luck.** Sparkplug BIRTH/DATA messages are never retained (per spec, and confirmed in our own
   `publish()`), so a subscriber that isn't listening yet simply never sees them — there's no
   redelivery. Our own service connects and publishes in about a millisecond, while the JVM-based
   `tahu-host-compat` takes a few hundred ms just to start attempting its MQTT connection, so
   starting both at once would race, and the very first NBIRTH would frequently be lost before
   `tahu-host-compat` ever subscribed. The script starts `mosquitto` and `tahu-host-compat` first,
   waits for `tahu-host-compat`'s log to show it successfully subscribed to `spBv1.0/#`, and only
   then starts `sparkplug-export`.
1. **Our Edge Node's initial NBIRTH is accepted** — waits for `tahu-host-compat`'s log to show
   `onNodeBirthComplete from ...` (confirmed against `SparkplugHostApplication.java`'s actual
   event-handler log lines), and fails if a rebirth was requested first
   (`SequenceReorderManager.java`'s `"...equesting a rebirth..."` log lines — matches both
   `Requesting` and `requesting` cases in that source).
2. **A sample EdgeX event triggers a DBIRTH** — POSTs a synthetic `dtos.Event` to our service's
   HTTP trigger endpoint (`POST /api/v3/trigger`, the App Functions SDK's documented mechanism for
   feeding an event into the pipeline without a running EdgeX MessageBus/core-data), then waits
   for `onDeviceBirthComplete from ...` — the device's first sighting should auto-birth it.
3. **The same event again produces DDATA, not another DBIRTH** — same event, same metric names;
   `tahu-host-compat` should log `onDeviceDataArrived from ...` since the device is already born.
4. **Restarting our service produces a fresh, accepted NBIRTH** — `docker compose restart
   sparkplug-export` forces a brand new MQTT session; `tahu-host-compat` should show a fresh
   `onNodeBirthComplete` with no rebirth request. This also exercises the bdSeq-persistence fix
   (`internal/sparkplug/node.go`'s `nextBdSeq`): the compose file sets
   `SPARKPLUG_BDSEQSTATEPATH=/tmp/sparkplug-bdseq`, which survives a `restart` (same
   container, same filesystem) even though it wouldn't survive a full recreate.

Each check only looks at *new* log lines since the last checkpoint (tracked by line count), so a
later step can't accidentally pass by matching an earlier step's log line.

## Why `tahu-host-compat` shares Mosquitto's network namespace

`tahu-host-compat` is Tahu's own demo/reference app: its broker URL (`tcp://localhost:1883`) and
credentials (`admin`/`changeme`) are hardcoded Java constants in
`SparkplugHostApplication.java`, not externally configurable via env vars or a properties file.
Rather than patch the reference implementation — which would undermine using it as an independent
oracle — its `docker-compose.yml` service uses `network_mode: "service:mosquitto"` so
`localhost:1883` resolves correctly from inside its container, and `mosquitto.conf` is configured
with a matching `admin`/`changeme` user (generated fresh at container start, not committed to the
repo). Our own service joins the same network namespace for the same reason: the
`sparkplug-export` profile's default `Sparkplug.MqttBroker.Url` is also `tcp://localhost:1883`.

## Extending this manually: exercising the NCMD rebirth-on-command path

The scripted test above exercises rebirth-on-reconnect (step 4), but not an explicit inbound NCMD
`Node Control/Rebirth` command — that path is already covered thoroughly by
`node_test.go`'s `TestHandleNCmd_RebirthCommandRepublishesBirths` against our own fakes. To
exercise it against `tahu-host-compat` specifically (as the thing *sending* the command) instead:

`tahu-host-compat` polls `/tmp/commands` (mounted as the `tahu-commands` volume in
`docker-compose.yml`) every 50ms for a JSON file describing a message to publish — see
`CommandListener.java` and `MessageUtil.fromJsonString` in the Tahu source for the exact JSON
shape it expects (a `{"topic": ..., "payload": ...}` document matching Tahu's own `Message`/
`SparkplugBPayload` Jackson serialization). Dropping a file there containing an NCMD
`Node Control/Rebirth = true` message to our Edge Node's NCMD topic
(`spBv1.0/{group}/NCMD/{edgeNode}`) should trigger the same fresh-NBIRTH-and-DBIRTH-replay
behavior verified in `node_test.go`, this time confirmed against the reference host application
actually sending the command over the wire rather than our own test fakes constructing it
in-process.
