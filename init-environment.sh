#!/usr/bin/env bash
# Resets the environment, then reloads the sample files. Deletes the data of:
#   1. PostgreSQL  postgres/cleanup_all_tables.sql, in the DB_NAME database,
#                  then postgres/create_views.sql to (re)create the views the
#                  connectors read
#   2. DynamoDB    dynamodb/delete-table-data.sh
#   3. MongoDB     mongodb/clear-bigdata-mongo.sh
#   4. Kafka       cancels the Flink jobs (flink/submit-jobs.sh --cancel), then
#                  kafka-connectors/deploy-connectors.sh -y, which destroys
#                  every connector, topic and schema, then creates them again
#   5. Flink       flink/submit-jobs.sh -y, which empties the jobs' output
#                  topics and submits the jobs again
# Tables, collections, indexes and views are kept; only the data is removed.
# Then:
#   6. S3          s3/upload-sample-files.sh, which uploads data/samples to the
#                  landing bucket and so starts the load into DynamoDB
#
# Usage: ./init-environment.sh
#
# Asks once before PostgreSQL, DynamoDB and Kafka; the MongoDB script then
# asks for its own confirmation (type "bigdata"). Stops at the first step
# that fails.
# Requires docker, aws, jq, curl and mongosh, a terminal, and the stack to be up.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
POSTGRES_CONTAINER="bigdata-postgres"
POSTGRES_SQL="$SCRIPT_DIR/postgres/cleanup_all_tables.sql"
POSTGRES_VIEWS_SQL="$SCRIPT_DIR/postgres/create_views.sql"
DYNAMODB_SCRIPT="$SCRIPT_DIR/dynamodb/delete-table-data.sh"
MONGODB_SCRIPT="$SCRIPT_DIR/mongodb/clear-bigdata-mongo.sh"
KAFKA_SCRIPT="$SCRIPT_DIR/kafka-connectors/deploy-connectors.sh"
FLINK_SCRIPT="$SCRIPT_DIR/flink/submit-jobs.sh"
S3_SCRIPT="$SCRIPT_DIR/s3/upload-sample-files.sh"

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

# Runs a SQL file in one transaction: it applies everything or nothing
run_postgres_sql() {
  docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 --single-transaction -f - < "$1"
}

clean_postgres() {
  run_postgres_sql "$POSTGRES_SQL" || return 1
  # The views can be missing (dropped along with the tables when the schema
  # is rebuilt); the connectors of the Kafka step cannot start without them
  log "Creating the views"
  run_postgres_sql "$POSTGRES_VIEWS_SQL"
}

clean_dynamodb() {
  # Already confirmed above, so the script's own question is skipped
  "$DYNAMODB_SCRIPT" -y
}

clean_mongodb() {
  "$MONGODB_SCRIPT"
}

# Last, so the recreated connectors start on empty views and empty topics
reset_kafka() {
  # The Flink jobs read these topics: left running, they would make the
  # broker re-create each topic the moment it is deleted
  "$FLINK_SCRIPT" --cancel || return 1
  "$KAFKA_SCRIPT" -y
}

# Once the source topics are back: fresh jobs on empty output topics
restart_flink() {
  "$FLINK_SCRIPT" -y
}

# After everything is empty and the connectors are back: starts a fresh load
upload_samples() {
  "$S3_SCRIPT"
}

if [ ! -f "$ENV_FILE" ]; then
  fail "$ENV_FILE not found"
  exit 1
fi
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

for tool in docker aws jq curl mongosh; do
  if ! command -v "$tool" > /dev/null; then
    fail "$tool is required"
    exit 1
  fi
done
for file in "$POSTGRES_SQL" "$POSTGRES_VIEWS_SQL" "$DYNAMODB_SCRIPT" "$MONGODB_SCRIPT" "$KAFKA_SCRIPT" "$FLINK_SCRIPT" "$S3_SCRIPT"; do
  if [ ! -f "$file" ]; then
    fail "$file not found"
    exit 1
  fi
done
if [ -z "${POSTGRES_USER:-}" ] || [ -z "${DB_NAME:-}" ]; then
  fail "POSTGRES_USER and DB_NAME must be set in $ENV_FILE"
  exit 1
fi
if ! docker exec "$POSTGRES_CONTAINER" true 2> /dev/null; then
  fail "container $POSTGRES_CONTAINER is not running"
  exit 1
fi
if [ ! -t 0 ]; then
  fail "run this script from a terminal: the MongoDB step needs a typed confirmation"
  exit 1
fi

log "This deletes ALL data from:"
log "  1. PostgreSQL  database $DB_NAME, schema commercialloan (views are recreated)"
log "  2. DynamoDB    the tables listed in dynamodb/delete-table-data.sh"
log "  3. MongoDB     database bigdata"
log "  4. Kafka       every connector, topic and schema in kafka-connectors (then recreated)"
log "  5. Flink       the output topics of the jobs in flink/jobs (jobs are restarted)"
log "and then uploads the files of data/samples to S3, which starts a new load."
if ! confirm "Continue?"; then
  log "Nothing deleted"
  exit 0
fi

step=0
for name in PostgreSQL DynamoDB MongoDB Kafka Flink S3; do
  step=$((step + 1))
  log ""
  log "== $step/6 $name"
  case "$name" in
    PostgreSQL) clean_postgres ;;
    DynamoDB)   clean_dynamodb ;;
    MongoDB)    clean_mongodb ;;
    Kafka)      reset_kafka ;;
    Flink)      restart_flink ;;
    S3)         upload_samples ;;
  esac
  status=$?
  if [ "$status" -ne 0 ]; then
    fail "$name step failed (exit $status); the following steps were not run"
    exit 1
  fi
done

log ""
log "Environment initialized: stores and topics were emptied, and the sample files are uploaded"
