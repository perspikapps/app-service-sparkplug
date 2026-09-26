#!/usr/bin/env bash
#
# Runs the Sparkplug B compliance test: brings up this repo's sparkplug-export service, a local
# Mosquitto broker, and Eclipse Tahu's reference Host Application (tahu-host-compat), then checks
# that the reference implementation accepts our NBIRTH/DBIRTH/DDATA cleanly (no rebirth requests).
#
# See docs/testing/tahu-compliance-testing.md for what each check means and how to extend this
# manually (e.g. exercising the NCMD rebirth-on-command path).
#
# Usage: ./run-compliance-test.sh
# Exit code 0 = all checks passed. Non-zero = a check failed; see the printed FAIL line and
# compliance-test.log (written on exit) for the full container logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
cd "$SCRIPT_DIR"

COMPOSE="docker compose"
BIRTH_TIMEOUT_SECS=90
EVENT_TIMEOUT_SECS=30

cleanup() {
  local exit_code=$?
  echo "==> Saving container logs to compliance-test.log"
  $COMPOSE logs --no-color >compliance-test.log 2>&1 || true
  echo "==> Tearing down"
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$exit_code"
}
trap cleanup EXIT

log_line_count() {
  $COMPOSE logs "$1" 2>/dev/null | wc -l | tr -d ' '
}

# wait_for_log polls $1's logs (only lines after line number $4, default 0) for $2, up to $3
# seconds.
#
# Logs are captured into a variable before grepping: piping straight into `grep -q` under
# `set -o pipefail` lets grep exit on the first match, killing the upstream command with SIGPIPE and
# making the whole pipeline report failure exactly when the pattern *was* found.
wait_for_log() {
  local service="$1" pattern="$2" timeout="$3" since_line="${4:-0}"
  local waited=0 out
  while ((waited < timeout)); do
    out="$($COMPOSE logs "$service" 2>/dev/null | tail -n "+$((since_line + 1))" || true)"
    if grep -q "$pattern" <<<"$out"; then
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  return 1
}

fail_if_rebirth_requested() {
  local out
  out="$($COMPOSE logs tahu-host-compat 2>/dev/null || true)"
  if grep -qi "equesting a rebirth" <<<"$out"; then
    echo "FAIL: tahu-host-compat requested a rebirth - our Edge Node's BIRTH/DATA was not accepted as spec-compliant."
    exit 1
  fi
}

echo "==> Building and starting mosquitto and tahu-host-compat"
$COMPOSE up -d --build mosquitto tahu-host-compat

# tahu-host-compat must be connected and subscribed to spBv1.0/# *before* sparkplug-export starts:
# Sparkplug BIRTH/DATA messages are never retained (per spec, and confirmed in our own publish()),
# so if sparkplug-export published its initial NBIRTH before tahu-host-compat's subscription was
# active, that NBIRTH would be lost to the void - a real race, not a config issue, since our lean
# Go binary connects and publishes in ~1ms while the JVM host app takes several hundred ms just to
# start attempting its own MQTT connect.
echo "==> Waiting for tahu-host-compat to connect and subscribe"
if ! wait_for_log tahu-host-compat "server Mqtt Server One - Successfully subscribed on \[spBv1.0/#" "$BIRTH_TIMEOUT_SECS"; then
  echo "FAIL: tahu-host-compat never subscribed to the Sparkplug topic namespace."
  exit 1
fi

echo "==> Building and starting sparkplug-export"
$COMPOSE up -d --build sparkplug-export

echo "==> Waiting for our Edge Node's NBIRTH to be accepted"
if ! wait_for_log tahu-host-compat "onNodeBirthComplete from" "$BIRTH_TIMEOUT_SECS"; then
  echo "FAIL: tahu-host-compat never confirmed our Edge Node's NBIRTH."
  exit 1
fi
fail_if_rebirth_requested
echo "    OK: onNodeBirthComplete observed, no rebirth requested"

MOSQUITTO_CID="$($COMPOSE ps -q mosquitto)"
EVENT_ID="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
READING_ID="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
ORIGIN_NS="$(date +%s%N)"
EVENT_JSON=$(printf '{"apiVersion":"v3","id":"%s","deviceName":"ComplianceTestDevice","profileName":"ComplianceTestProfile","sourceName":"temperature","origin":%s,"readings":[{"id":"%s","origin":%s,"deviceName":"ComplianceTestDevice","resourceName":"temperature","profileName":"ComplianceTestProfile","valueType":"Float64","value":"21.5"}]}' \
  "$EVENT_ID" "$ORIGIN_NS" "$READING_ID" "$ORIGIN_NS")

# Runs a throwaway curl container in mosquitto's network namespace (shared with our own service -
# see docker-compose.yml) to POST a sample EdgeX event to our service's HTTP trigger endpoint,
# without publishing any port to the host.
post_sample_event() {
  docker run --rm --network "container:$MOSQUITTO_CID" curlimages/curl:8.10.1 \
    -sf -X POST "http://localhost:59708/api/v3/trigger" \
    -H "Content-Type: application/json" \
    --data-raw "$EVENT_JSON" >/dev/null
}

BASELINE="$(log_line_count tahu-host-compat)"
echo "==> Triggering a sample EdgeX event (expect a DBIRTH: first sighting of this device)"
post_sample_event
if ! wait_for_log tahu-host-compat "onDeviceBirthComplete from" "$EVENT_TIMEOUT_SECS" "$BASELINE"; then
  echo "FAIL: tahu-host-compat never confirmed the device's DBIRTH."
  exit 1
fi
fail_if_rebirth_requested
echo "    OK: onDeviceBirthComplete observed, no rebirth requested"

BASELINE="$(log_line_count tahu-host-compat)"
echo "==> Triggering the same event again (expect DDATA: device already born)"
post_sample_event
if ! wait_for_log tahu-host-compat "onDeviceDataArrived from" "$EVENT_TIMEOUT_SECS" "$BASELINE"; then
  echo "FAIL: tahu-host-compat never confirmed a DDATA for the device."
  exit 1
fi
fail_if_rebirth_requested
echo "    OK: onDeviceDataArrived observed, no rebirth requested"

BASELINE="$(log_line_count tahu-host-compat)"
echo "==> Restarting our service (exercises a fresh MQTT session and bdSeq persistence)"
$COMPOSE restart sparkplug-export
if ! wait_for_log tahu-host-compat "onNodeBirthComplete from" "$BIRTH_TIMEOUT_SECS" "$BASELINE"; then
  echo "FAIL: tahu-host-compat never confirmed a fresh NBIRTH after restart."
  exit 1
fi
fail_if_rebirth_requested
echo "    OK: fresh NBIRTH observed after restart, no rebirth requested"

echo "==> ALL CHECKS PASSED"
