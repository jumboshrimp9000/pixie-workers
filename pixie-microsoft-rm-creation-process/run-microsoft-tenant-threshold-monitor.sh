#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_ROOT="${THRESHOLD_MONITOR_LOG_ROOT:-$SCRIPT_DIR/logs/tenant-threshold-monitor}"
STATE_FILE="$LOG_ROOT/.last-success"
LOCK_FILE="${THRESHOLD_MONITOR_LOCK_FILE:-/var/lock/simpleinboxes-tenant-threshold-monitor.lock}"
MIN_INTERVAL_SECONDS="${THRESHOLD_MONITOR_MIN_INTERVAL_SECONDS:-169200}"
SHARD_COUNT="${THRESHOLD_MONITOR_SHARD_COUNT:-4}"
TRACE_POLLS="${THRESHOLD_MONITOR_TRACE_POLLS:-4}"
TRACE_POLL_SECONDS="${THRESHOLD_MONITOR_TRACE_POLL_SECONDS:-60}"
RECIPIENT="${THRESHOLD_MONITOR_RECIPIENT:-leads+99@justresultsagency.com}"
RUN_ID="${THRESHOLD_MONITOR_RUN_ID:-tenant-threshold-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -u +%FT%TZ)] Tenant threshold monitor already running; exiting."
  exit 0
fi

now_epoch="$(date -u +%s)"
if [[ -f "$STATE_FILE" ]]; then
  last_epoch="$(cat "$STATE_FILE" || echo 0)"
  if [[ "$last_epoch" =~ ^[0-9]+$ ]]; then
    elapsed=$((now_epoch - last_epoch))
    if (( elapsed < MIN_INTERVAL_SECONDS )); then
      echo "[$(date -u +%FT%TZ)] Last successful threshold monitor ran ${elapsed}s ago; waiting for ${MIN_INTERVAL_SECONDS}s interval."
      exit 0
    fi
  fi
fi

echo "[$(date -u +%FT%TZ)] Starting tenant threshold monitor run_id=$RUN_ID shard_count=$SHARD_COUNT"
pwsh -NoLogo -NoProfile -File "$SCRIPT_DIR/Invoke-MicrosoftTenantThresholdMonitor.ps1" \
  -RunId "$RUN_ID" \
  -ShardCount "$SHARD_COUNT" \
  -TracePolls "$TRACE_POLLS" \
  -TracePollSeconds "$TRACE_POLL_SECONDS" \
  -Recipient "$RECIPIENT"

date -u +%s > "$STATE_FILE"
echo "[$(date -u +%FT%TZ)] Tenant threshold monitor completed run_id=$RUN_ID"
