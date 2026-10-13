#!/usr/bin/env bash
# Publishes near-real-time agreement balances to Kafka: 2 balances every
# 2 seconds for one minute (60 messages), on existing agreement numbers read
# from commercialloan.tbl_agreement.
#
# Usage: ./send-nrt-balances.sh
#
# Each message has the agreement number as key and, as value, an Event in
# JSON: {"id", "eventName", "source", "payload"}. The payload is a string
# holding one balance, shaped like an "agmtBal" entry of the UNBNK_BALANCE
# files. Override the defaults with TOPIC, EVENT_NAME, EVENT_SOURCE,
# BALANCES_PER_TICK, INTERVAL_SECONDS, DURATION_SECONDS.
# Requires docker, jq and uuidgen, and the stack to be up.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

TOPIC="${TOPIC:-3857.dev.nrt.balances.info}"
EVENT_NAME="${EVENT_NAME:-balance.updated}"
EVENT_SOURCE="${EVENT_SOURCE:-LoanIQ}"
BALANCES_PER_TICK="${BALANCES_PER_TICK:-2}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-2}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
PRODUCER_STARTUP_SECONDS=4
KAFKA_CONTAINER="bigdata-kafka"
POSTGRES_CONTAINER="bigdata-postgres"
TAB=$'\t'

log()  { printf '%s\n' "$*" >&2; }
fail() { printf 'ERROR: %s\n' "$*" >&2; }

# Prints "agreement_no <tab> original_amt <tab> currency", in random order
load_agreements() {
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$DB_NAME" -At -F "$TAB" -c \
    "select agreement_no, coalesce(original_amt, 0), coalesce(currency, 'CAD')
       from commercialloan.tbl_agreement
      order by random()"
}

# Builds one balance: the current amount is a random 10-100% of the original
build_balance() {
  local agreement_no="$1" original_amt="$2" currency="$3"
  jq -cn --arg agmtId "$agreement_no" --arg currency "$currency" \
    --argjson original "$original_amt" --argjson pct "$((RANDOM % 91 + 10))" '
    (($original * $pct) | round / 100) as $current
    | {
        agmtId: $agmtId,
        currency: $currency,
        fxRate: 1.0,
        globalOriginalAgmtAmt: $original,
        globalCurrentAgmtAmt: $current,
        bankGrossAgmtAmt: $current,
        bankNetAgmtAmt: $current,
        participantAgmtAmt: 0.0,
        ourSyndicationDetentionPct: 100,
        previousAgmtBal: null
      }'
}

# Wraps a balance in an Event; payload is the balance serialized as a string
build_event() {
  local balance="$1" id
  id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  jq -cn --arg id "$id" --arg eventName "$EVENT_NAME" --arg source "$EVENT_SOURCE" \
    --arg payload "$balance" \
    '{id: $id, eventName: $eventName, source: $source, payload: $payload}'
}

# Writes "key <tab> value" lines on stdout, paced by INTERVAL_SECONDS
generate() {
  local ticks=$((DURATION_SECONDS / INTERVAL_SECONDS)) tick=0 index=0 sent=0 i
  local agreement_no original_amt currency balance
  # Lets the producer finish starting, so the first balances are not held back
  sleep "$PRODUCER_STARTUP_SECONDS"
  while [ "$tick" -lt "$ticks" ]; do
    i=0
    while [ "$i" -lt "$BALANCES_PER_TICK" ]; do
      # Start over from the first agreement once they have all been used
      IFS="$TAB" read -r agreement_no original_amt currency <<< "${agreements[$((index % ${#agreements[@]}))]}"
      balance="$(build_balance "$agreement_no" "$original_amt" "$currency")"
      printf '%s\t%s\n' "$agreement_no" "$(build_event "$balance")"
      log "  $(date +%H:%M:%S)  sent balance for $agreement_no"
      index=$((index + 1))
      sent=$((sent + 1))
      i=$((i + 1))
    done
    tick=$((tick + 1))
    [ "$tick" -lt "$ticks" ] && sleep "$INTERVAL_SECONDS"
  done
  log "$sent balance(s) sent"
}

for tool in docker jq uuidgen; do
  if ! command -v "$tool" > /dev/null; then
    fail "$tool is required"
    exit 1
  fi
done
if [ -z "${POSTGRES_USER:-}" ] || [ -z "${DB_NAME:-}" ]; then
  fail "POSTGRES_USER and DB_NAME must be set in $ENV_FILE"
  exit 1
fi

agreements=()
while IFS= read -r line; do
  [ -n "$line" ] && agreements+=("$line")
done < <(load_agreements)
if [ "${#agreements[@]}" -eq 0 ]; then
  fail "no agreement found in commercialloan.tbl_agreement of $DB_NAME"
  exit 1
fi

if ! docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 > /dev/null; then
  fail "could not create topic $TOPIC"
  exit 1
fi

log "Sending $BALANCES_PER_TICK balance(s) every ${INTERVAL_SECONDS}s for ${DURATION_SECONDS}s to $TOPIC (${#agreements[@]} agreements available)"

# One producer stays open for the whole run; --sync and --timeout 0 send
# each line as soon as it is written instead of batching for a second
generate | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" --sync --timeout 0 \
  --property parse.key=true --property "key.separator=$TAB"
status=("${PIPESTATUS[@]}")
if [ "${status[0]}" -ne 0 ] || [ "${status[1]}" -ne 0 ]; then
  fail "sending failed (generator exit ${status[0]}, producer exit ${status[1]})"
  exit 1
fi
