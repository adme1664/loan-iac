#!/usr/bin/env bash
# Deletes every item of the DynamoDB tables listed below. The tables
# themselves (keys, indexes, settings) are kept.
#
# Usage: ./delete-table-data.sh [-y] [table ...]
#   ./delete-table-data.sh                      empty every table in TABLES
#   ./delete-table-data.sh unbnk_deal_dev       empty one table
#   ./delete-table-data.sh -y                   do not ask for confirmation
#
# Uses the AWS CLI's current credentials and region (AWS_PROFILE, AWS_REGION).
# Set DYNAMODB_ENDPOINT_URL to target a local DynamoDB instead of AWS.
# Requires aws and jq.

set -uo pipefail

TABLES=(
  unbnk_agreement_dev
  unbnk_balance_dev
  unbnk_deal_dev
  unbnk_schedule_dev
)

BATCH_SIZE=25   # maximum accepted by BatchWriteItem
MAX_RETRIES=8

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; }

ddb() {
  if [ -n "${DYNAMODB_ENDPOINT_URL:-}" ]; then
    aws dynamodb "$@" --endpoint-url "$DYNAMODB_ENDPOINT_URL"
  else
    aws dynamodb "$@"
  fi
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

# Sends one batch of delete requests, retrying whatever DynamoDB left unprocessed
delete_batch() {
  local request="$1" attempt=0 response
  while :; do
    if ! response="$(ddb batch-write-item --request-items "$request" --output json)"; then
      return 1
    fi
    request="$(printf '%s' "$response" | jq -c '.UnprocessedItems // {}')"
    [ "$request" = "{}" ] && return 0
    attempt=$((attempt + 1))
    if [ "$attempt" -gt "$MAX_RETRIES" ]; then
      fail "items still unprocessed after $MAX_RETRIES retries"
      return 1
    fi
    sleep "$attempt"
  done
}

empty_table() {
  local table="$1" keys names projection items total deleted=0 request

  # Key attribute names, e.g. ["pk","sk"]
  if ! keys="$(ddb describe-table --table-name "$table" --output json |
    jq -c '[.Table.KeySchema[].AttributeName]')" || [ -z "$keys" ]; then
    fail "could not describe table $table"
    return 1
  fi

  # Aliases (#k0, #k1) keep the scan valid when a key name is a reserved word
  names="$(printf '%s' "$keys" | jq -c 'to_entries | map({key: "#k\(.key)", value: .value}) | from_entries')"
  projection="$(printf '%s' "$keys" | jq -r 'to_entries | map("#k\(.key)") | join(",")')"

  items="$(mktemp)"
  if ! ddb scan --table-name "$table" --consistent-read \
    --projection-expression "$projection" \
    --expression-attribute-names "$names" --output json > "$items"; then
    fail "could not scan table $table"
    rm -f "$items"
    return 1
  fi

  total="$(jq '.Items | length' "$items")"
  if [ "$total" -eq 0 ]; then
    log "  already empty"
    rm -f "$items"
    return 0
  fi

  while IFS= read -r request; do
    if ! delete_batch "$request"; then
      fail "stopped after $deleted of $total items in $table"
      rm -f "$items"
      return 1
    fi
    deleted=$((deleted + $(printf '%s' "$request" | jq '.[] | length')))
    printf '\r  deleted %s of %s' "$deleted" "$total"
  done < <(jq -c --arg table "$table" --argjson size "$BATCH_SIZE" '
    [.Items[] | {DeleteRequest: {Key: .}}]
    | range(0; length; $size) as $i
    | {($table): .[$i:$i + $size]}' "$items")
  printf '\n'
  rm -f "$items"
}

assume_yes=false
if [ "${1:-}" = "-y" ]; then
  assume_yes=true
  shift
fi
[ "$#" -gt 0 ] && TABLES=("$@")

for tool in aws jq; do
  if ! command -v "$tool" > /dev/null; then
    fail "$tool is required"
    exit 1
  fi
done

log "Target:  ${DYNAMODB_ENDPOINT_URL:-AWS, region $(aws configure get region 2> /dev/null || echo "${AWS_REGION:-unknown}")}"
log "Tables:  ${TABLES[*]}"
if [ "$assume_yes" != true ] && ! confirm "Delete ALL items from these tables?"; then
  log "Nothing deleted"
  exit 0
fi

failed=()
for table in "${TABLES[@]}"; do
  log ""
  log "== $table"
  empty_table "$table" || failed+=("$table")
done

log ""
if [ "${#failed[@]}" -gt 0 ]; then
  fail "${#failed[@]} of ${#TABLES[@]} failed: ${failed[*]}"
  exit 1
fi
log "All ${#TABLES[@]} tables emptied"
