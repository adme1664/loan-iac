#!/usr/bin/env bash
# Submits the Flink SQL jobs of the jobs folder to the Flink cluster.
#
# Usage: ./submit-jobs.sh [-y | --cancel] [job-file.sql ...]
#   ./submit-jobs.sh                            submit every jobs/*.sql
#   ./submit-jobs.sh credit-facility-info.sql   submit one job
#   ./submit-jobs.sh -y                         restart without asking: cancel
#                                               the running job, empty its
#                                               output topic, submit it again
#   ./submit-jobs.sh --cancel                   only cancel the running jobs
#
# A job is identified by the 'pipeline.name' set in its SQL file. If a job of
# that name is already running, the script asks whether to restart it (no =
# leave it running). Output topics are the upsert-kafka sinks' topics of the
# file: they are created if missing, and deleted and recreated on a restart
# so the restarted job publishes into an empty topic.
# Requires docker, curl and jq, and the stack to be up.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

FLINK_URL="http://localhost:${FLINK_PORT:-8088}"
JOBMANAGER_CONTAINER="bigdata-flink-jobmanager"
KAFKA_CONTAINER="bigdata-kafka"
JOBS_DIR="$SCRIPT_DIR/jobs"
WAIT_SECONDS=120

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; }

# Asks a yes/no question; anything but y/yes (or no input at all) means no
confirm() {
  local answer=""
  printf '%s [y/N] ' "$1"
  read -r answer || printf '\n'
  case "$answer" in
    [yY] | [yY][eE][sS]) return 0 ;;
    *) return 1 ;;
  esac
}

kafka_topics() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

topic_exists() {
  kafka_topics --list | grep -Fxq "$1"
}

create_topic() {
  kafka_topics --create --if-not-exists --topic "$1" --partitions 1 --replication-factor 1 > /dev/null
}

delete_topic() {
  local topic="$1" waited=0
  topic_exists "$topic" || return 0
  kafka_topics --delete --topic "$topic" > /dev/null || return 1
  # Deletion is asynchronous: wait until the broker no longer lists it
  while topic_exists "$topic"; do
    [ "$waited" -ge "$WAIT_SECONDS" ] && return 1
    sleep 2
    waited=$((waited + 2))
  done
}

# Prints the value of SET 'pipeline.name' = '...' in a SQL file
job_name() {
  sed -n "s/^[[:space:]]*SET[[:space:]]*'pipeline\.name'[[:space:]]*=[[:space:]]*'\([^']*\)'.*/\1/p" "$1" | head -1
}

# Prints the topic of each sink table of the file. Convention: a table is a
# sink when its name ends with _sink
sink_topics() {
  awk '
    /^[[:space:]]*CREATE TABLE/ { is_sink = ($3 ~ /_sink$/) }
    is_sink && /\047topic\047/ {
      line = $0
      sub(/.*\047topic\047[[:space:]]*=[[:space:]]*\047/, "", line)
      sub(/\047.*/, "", line)
      print line
    }' "$1"
}

# Prints the ids of the jobs of that name that are not finished
active_job_ids() {
  curl -s -m 10 "$FLINK_URL/jobs/overview" | jq -r --arg name "$1" '
    .jobs[] | select(.name == $name)
    | select(.state | IN("RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "FAILING"))
    | .jid'
}

cancel_job() {
  local id="$1" waited=0 state
  curl -s -m 10 -o /dev/null -X PATCH "$FLINK_URL/jobs/$id?mode=cancel"
  while :; do
    state="$(curl -s -m 10 "$FLINK_URL/jobs/$id" | jq -r '.state')"
    [ "$state" = "CANCELED" ] && return 0
    [ "$waited" -ge "$WAIT_SECONDS" ] && return 1
    sleep 2
    waited=$((waited + 2))
  done
}

wait_for_running() {
  local name="$1" waited=0 state
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    state="$(curl -s -m 10 "$FLINK_URL/jobs/overview" | jq -r --arg name "$name" '
      [.jobs[] | select(.name == $name)] | sort_by(.["start-time"]) | last | .state // "NONE"')"
    case "$state" in
      RUNNING)
        # A job can start, then fail on its first records: check once more
        sleep 8
        [ -n "$(active_job_ids "$name")" ] && [ "$(curl -s -m 10 "$FLINK_URL/jobs/overview" | jq -r --arg name "$name" '
          [.jobs[] | select(.name == $name)] | sort_by(.["start-time"]) | last | .state')" = "RUNNING" ] && return 0
        ;;
      FAILED | CANCELED) return 1 ;;
    esac
    sleep 3
    waited=$((waited + 3))
  done
  return 1
}

submit_job() {
  local file="$1" name ids id topic output
  name="$(job_name "$file")"
  if [ -z "$name" ]; then
    fail "$(basename "$file") must contain: SET 'pipeline.name' = '<job name>';"
    return 1
  fi

  ids="$(active_job_ids "$name")"
  if [ "$CANCEL_ONLY" = true ]; then
    if [ -z "$ids" ]; then
      log "  job        $name is not running"
      return 0
    fi
    for id in $ids; do
      cancel_job "$id" || { fail "could not cancel job $id"; return 1; }
    done
    log "  cancelled  $name"
    return 0
  fi
  if [ -n "$ids" ]; then
    log "  job        $name is already running"
    if [ "$ASSUME_YES" != true ] && ! confirm "  Cancel it, empty its output topic(s) and submit it again?"; then
      log "  kept as is (nothing changed)"
      return 0
    fi
    for id in $ids; do
      cancel_job "$id" || { fail "could not cancel job $id"; return 1; }
    done
    while IFS= read -r topic; do
      [ -n "$topic" ] || continue
      delete_topic "$topic" || { fail "could not delete topic $topic"; return 1; }
    done < <(sink_topics "$file")
    log "  cancelled  the running job, output topic(s) removed"
  fi

  while IFS= read -r topic; do
    [ -n "$topic" ] || continue
    log "  topic      $topic"
    create_topic "$topic" || { fail "could not create topic $topic"; return 1; }
  done < <(sink_topics "$file")

  log "  job        $name"
  if ! output="$(docker exec "$JOBMANAGER_CONTAINER" /opt/flink/bin/sql-client.sh \
    -f "/opt/flink/jobs/$(basename "$file")" 2>&1)" || printf '%s' "$output" | grep -q '\[ERROR\]'; then
    fail "submission of $(basename "$file") failed"
    printf '%s\n' "$output" | grep -A6 '\[ERROR\]' | head -20 >&2
    return 1
  fi
  if ! wait_for_running "$name"; then
    fail "job $name is not running; see $FLINK_URL"
    return 1
  fi
}

ASSUME_YES=false
CANCEL_ONLY=false
case "${1:-}" in
  -y)       ASSUME_YES=true; shift ;;
  --cancel) CANCEL_ONLY=true; shift ;;
esac

for tool in docker curl jq; do
  if ! command -v "$tool" > /dev/null; then
    fail "$tool is required"
    exit 1
  fi
done
if ! curl -fs -m 10 -o /dev/null "$FLINK_URL/overview"; then
  fail "Flink does not answer at $FLINK_URL"
  exit 1
fi

files=()
if [ "$#" -gt 0 ]; then
  for name in "$@"; do
    files+=("$JOBS_DIR/$(basename "$name")")
  done
else
  for file in "$JOBS_DIR"/*.sql; do
    [ -f "$file" ] && files+=("$file")
  done
fi
if [ "${#files[@]}" -eq 0 ]; then
  fail "no .sql file found in $JOBS_DIR"
  exit 1
fi

failed=()
for file in "${files[@]}"; do
  log ""
  log "== $(basename "$file")"
  if [ ! -f "$file" ]; then
    fail "$file not found"
    failed+=("$(basename "$file")")
  elif submit_job "$file"; then
    log "  OK"
  else
    failed+=("$(basename "$file")")
  fi
done

log ""
if [ "${#failed[@]}" -gt 0 ]; then
  fail "${#failed[@]} of ${#files[@]} failed: ${failed[*]}"
  exit 1
fi
log "All ${#files[@]} job(s) submitted"
