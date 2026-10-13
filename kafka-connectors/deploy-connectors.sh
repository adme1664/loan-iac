#!/usr/bin/env bash
# Deploys every entity folder that contains a config.json:
#   1. creates the Kafka topic
#   2. registers the JSON schema under <topic>-value
#   3. creates the connector, then waits for it to run
# If the connector already exists, asks first whether to remove it together
# with its read position, its topic and its schema, so that everything is
# recreated empty and re-read from the start (no = leave the entity as is).
#
# Usage: ./deploy-connectors.sh [-y] [entity-folder ...]
#   ./deploy-connectors.sh                  deploy every entity
#   ./deploy-connectors.sh loan-connector   deploy one entity
#   ./deploy-connectors.sh -y               full reset without asking: remove
#                                           each connector, topic and schema
#                                           (whether or not the connector
#                                           exists), then create them again
#
# Safe to re-run. Requires docker, curl and jq, and the stack to be up.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

CONNECT_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
REGISTRY_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8081}"
KAFKA_CONTAINER="bigdata-kafka"
TOPIC_PARTITIONS=1
TOPIC_REPLICATION=1
WAIT_SECONDS=120

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; }

wait_for_url() {
  local name="$1" url="$2" waited=0
  until curl -fs -m 5 -o /dev/null "$url"; do
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
      fail "$name did not answer at $url after ${WAIT_SECONDS}s"
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
  log "  $name is up"
}

create_topic() {
  local topic="$1"
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions "$TOPIC_PARTITIONS" \
    --replication-factor "$TOPIC_REPLICATION" > /dev/null
}

topic_exists() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list | grep -Fxq "$1"
}

# Prints the broker's internal id of a topic, or nothing if it does not exist
topic_id() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic "$1" 2> /dev/null |
    sed -n 's/.*TopicId: \([^[:space:]]*\).*/\1/p' | head -1
}

delete_topic() {
  local topic="$1" waited=0 old_id
  old_id="$(topic_id "$topic")"
  [ -n "$old_id" ] || return 0
  if ! docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic" > /dev/null; then
    fail "could not delete topic $topic"
    return 1
  fi
  # Deletion is asynchronous: wait until that topic is gone. A client still
  # subscribed to it (a Flink job, a consumer) makes the broker re-create it
  # at once, empty, under a new id: that also counts as deleted.
  while [ "$(topic_id "$topic")" = "$old_id" ]; do
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
      fail "topic $topic was still present after ${WAIT_SECONDS}s"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# Removes every version of the subject: soft delete first, then permanent
delete_schema() {
  local subject="$1" code
  for query in "" "?permanent=true"; do
    code="$(curl -s -m 30 -o /dev/null -w '%{http_code}' -X DELETE "$REGISTRY_URL/subjects/$subject$query")"
    if [ "$code" != "200" ] && [ "$code" != "404" ]; then
      fail "schema removal returned HTTP $code"
      return 1
    fi
  done
}

register_schema() {
  local subject="$1" schema_file="$2" response code
  # The registry's default BACKWARD check rejects a schema that gains fields
  # (HTTP 409). The schema files are the source of truth here, so the check
  # is turned off for the subject before registering.
  code="$(curl -s -m 30 -o /dev/null -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"compatibility": "NONE"}' "$REGISTRY_URL/config/$subject")"
  if [ "$code" != "200" ]; then
    fail "could not set the compatibility of $subject (HTTP $code)"
    return 1
  fi
  response="$(jq -c '{schemaType: "JSON", schema: tojson}' "$schema_file" |
    curl -s -m 30 -w '\n%{http_code}' -X POST \
      -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data @- "$REGISTRY_URL/subjects/$subject/versions")"
  code="${response##*$'\n'}"
  if [ "$code" != "200" ]; then
    fail "schema registration returned HTTP $code: ${response%$'\n'*}"
    return 1
  fi
}

connector_exists() {
  [ "$(curl -s -m 10 -o /dev/null -w '%{http_code}' "$CONNECT_URL/connectors/$1")" = "200" ]
}

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

# Removes the connector and its stored read position, so that a connector
# recreated under the same name reads its source from the beginning
delete_connector() {
  local name="$1" code response waited=0

  # Offsets can only be reset while the connector is stopped
  code="$(curl -s -m 30 -o /dev/null -w '%{http_code}' -X PUT "$CONNECT_URL/connectors/$name/stop")"
  if [ "$code" != "202" ] && [ "$code" != "204" ]; then
    fail "connector stop returned HTTP $code"
    return 1
  fi
  until [ "$(curl -s -m 10 "$CONNECT_URL/connectors/$name/status" | jq -r '.connector.state' 2> /dev/null)" = "STOPPED" ]; do
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
      fail "connector $name was not stopped after ${WAIT_SECONDS}s"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done

  response="$(curl -s -m 60 -w '\n%{http_code}' -X DELETE "$CONNECT_URL/connectors/$name/offsets")"
  code="${response##*$'\n'}"
  if [ "$code" != "200" ]; then
    fail "offset reset returned HTTP $code: ${response%$'\n'*}"
    return 1
  fi

  waited=0
  code="$(curl -s -m 30 -o /dev/null -w '%{http_code}' -X DELETE "$CONNECT_URL/connectors/$name")"
  if [ "$code" != "204" ]; then
    fail "connector removal returned HTTP $code"
    return 1
  fi
  # Removal is asynchronous: wait until the worker no longer lists it
  while connector_exists "$name"; do
    if [ "$waited" -ge "$WAIT_SECONDS" ]; then
      fail "connector $name was still present after ${WAIT_SECONDS}s"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

put_connector() {
  local name="$1" connector_file="$2" response code
  response="$(jq -c '.config' "$connector_file" |
    curl -s -m 60 -w '\n%{http_code}' -X PUT \
      -H 'Content-Type: application/json' \
      --data @- "$CONNECT_URL/connectors/$name/config")"
  code="${response##*$'\n'}"
  if [ "$code" != "200" ] && [ "$code" != "201" ]; then
    fail "connector creation returned HTTP $code: ${response%$'\n'*}"
    return 1
  fi
}

# Prints RUNNING, FAILED or PENDING for the connector and its tasks together
connector_state() {
  curl -s -m 10 "$CONNECT_URL/connectors/$1/status" | jq -r '
    ([.connector.state] + [.tasks[]?.state]) as $states
    | if ($states | index("FAILED")) then "FAILED"
      elif ((.tasks // []) | length) > 0 and ($states | all(. == "RUNNING")) then "RUNNING"
      else "PENDING" end' 2> /dev/null
}

print_connector_error() {
  curl -s -m 10 "$CONNECT_URL/connectors/$1/status" |
    jq -r '[.connector.trace, .tasks[]?.trace] | map(select(. != null)) | .[]' |
    grep -E '^(Caused by: )?[A-Za-z0-9_.$]+(Exception|Error)' | head -5 >&2
}

wait_for_connector() {
  local name="$1" waited=0 state
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    state="$(connector_state "$name")"
    case "$state" in
      RUNNING)
        # A task can start, then fail on its first record: check once more
        sleep 5
        [ "$(connector_state "$name")" = "RUNNING" ] && return 0
        ;;
      FAILED)
        fail "connector $name failed"
        print_connector_error "$name"
        return 1
        ;;
    esac
    sleep 3
    waited=$((waited + 3))
  done
  fail "connector $name was not running after ${WAIT_SECONDS}s"
  print_connector_error "$name"
  return 1
}

deploy_entity() {
  local dir="$1" config="$1/config.json"
  local topic connector_file schema_file connector_name

  if ! jq -e . "$config" > /dev/null 2>&1; then
    fail "$config is missing or is not valid JSON"
    return 1
  fi

  topic="$(jq -r '.topic // empty' "$config")"
  connector_file="$(jq -r '.connectorFile // empty' "$config")"
  schema_file="$(jq -r '.schemaFile // empty' "$config")"

  if [ -z "$topic" ] || [ -z "$connector_file" ]; then
    fail "$config must define topic and connectorFile"
    return 1
  fi
  connector_file="$dir/$connector_file"

  # Without a schemaFile key, use the folder's single *-schema.json
  if [ -n "$schema_file" ]; then
    schema_file="$dir/$schema_file"
  else
    set -- "$dir"/*-schema.json
    if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
      fail "expected exactly one *-schema.json in $dir (or set schemaFile in config.json)"
      return 1
    fi
    schema_file="$1"
  fi

  for file in "$connector_file" "$schema_file"; do
    if ! jq -e . "$file" > /dev/null 2>&1; then
      fail "$file is missing or is not valid JSON"
      return 1
    fi
  done

  connector_name="$(jq -r '.name // empty' "$connector_file")"
  if [ -z "$connector_name" ]; then
    fail "$connector_file must define name"
    return 1
  fi

  if [ "$ASSUME_YES" = true ]; then
    # Full reset: the topic and schema go even if the connector is already gone
    if connector_exists "$connector_name"; then
      delete_connector "$connector_name" || return 1
    fi
    delete_topic "$topic" || return 1
    delete_schema "$topic-value" || return 1
    log "  removed    connector, read position, topic and schema"
  elif connector_exists "$connector_name"; then
    log "  connector  $connector_name already exists"
    if confirm "  Remove the connector, its topic and its schema, then create them again?"; then
      delete_connector "$connector_name" || return 1
      delete_topic "$topic" || return 1
      delete_schema "$topic-value" || return 1
      log "  removed    connector, read position, topic and schema"
    else
      log "  kept as is (nothing changed)"
      return 0
    fi
  fi

  log "  topic      $topic"
  create_topic "$topic" || { fail "could not create topic $topic"; return 1; }

  log "  schema     $topic-value  ($(basename "$schema_file"))"
  register_schema "$topic-value" "$schema_file" || return 1

  log "  connector  $connector_name"
  put_connector "$connector_name" "$connector_file" || return 1
  wait_for_connector "$connector_name" || return 1
}

ASSUME_YES=false
if [ "${1:-}" = "-y" ]; then
  ASSUME_YES=true
  shift
fi

for tool in docker curl jq; do
  if ! command -v "$tool" > /dev/null; then
    fail "$tool is required"
    exit 1
  fi
done

log "Waiting for services"
docker exec "$KAFKA_CONTAINER" true 2> /dev/null || { fail "container $KAFKA_CONTAINER is not running"; exit 1; }
wait_for_url "Schema Registry" "$REGISTRY_URL/subjects" || exit 1
wait_for_url "Kafka Connect" "$CONNECT_URL/connectors" || exit 1

if [ "$#" -gt 0 ]; then
  entities=("$@")
else
  entities=()
  for config in "$SCRIPT_DIR"/*/config.json; do
    [ -f "$config" ] && entities+=("$(basename "$(dirname "$config")")")
  done
fi

if [ "${#entities[@]}" -eq 0 ]; then
  fail "no entity folder with a config.json found in $SCRIPT_DIR"
  exit 1
fi

failed=()
for entity in "${entities[@]}"; do
  entity="${entity%/}"
  log ""
  log "== $entity"
  if deploy_entity "$SCRIPT_DIR/$entity"; then
    log "  OK"
  else
    failed+=("$entity")
  fi
done

log ""
if [ "${#failed[@]}" -gt 0 ]; then
  fail "${#failed[@]} of ${#entities[@]} failed: ${failed[*]}"
  exit 1
fi
log "All ${#entities[@]} entities deployed"
