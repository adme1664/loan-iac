#!/usr/bin/env bash
# Uploads the UNBNK_*.json sample files to the S3 landing bucket. Each upload
# triggers the s3-files-extractor Lambda, which loads the file into DynamoDB.
#
# Usage: ./upload-sample-files.sh [folder]
#   ./upload-sample-files.sh                              data/samples, subfolders included
#   ./upload-sample-files.sh ../../data/samples/2026-10-11   one folder only
#
# Files land at the bucket root under their own name, whatever subfolder they
# come from. They are sent oldest business date first and, within a date, in
# the order deal, agreement, balance, schedule.
# Uses the AWS CLI's current credentials and region. Requires aws.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUCKET="${S3_BUCKET:-3857-mft-unbk-files}"
SAMPLES_DIR="${1:-$SCRIPT_DIR/../../data/samples}"

log()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; }

# Prints "<business date> <entity rank> <path>" for one file, or nothing if
# the name is not UNBNK_<ENTITY>_DG_<yyyymmdd>_DT_<yyyymmdd>.json
sort_key() {
  local path="$1" name entity date rank
  name="$(basename "$path")"
  case "$name" in
    UNBNK_*_DG_????????_DT_????????.json) ;;
    *) return 0 ;;
  esac
  entity="${name#UNBNK_}"
  entity="${entity%%_DG_*}"
  date="${name##*_DT_}"
  date="${date%.json}"
  case "$entity" in
    DEAL)      rank=1 ;;
    AGREEMENT) rank=2 ;;
    BALANCE)   rank=3 ;;
    SCHEDULE)  rank=4 ;;
    *)         rank=9 ;;
  esac
  printf '%s\t%s\t%s\n' "$date" "$rank" "$path"
}

if ! command -v aws > /dev/null; then
  fail "aws is required"
  exit 1
fi
if [ ! -d "$SAMPLES_DIR" ]; then
  fail "folder $SAMPLES_DIR not found"
  exit 1
fi
SAMPLES_DIR="$(cd "$SAMPLES_DIR" && pwd)"

files=()
while IFS=$'\t' read -r _ _ path; do
  files+=("$path")
done < <(find "$SAMPLES_DIR" -type f -name 'UNBNK_*.json' | while IFS= read -r path; do
  sort_key "$path"
done | sort -t $'\t' -k1,1 -k2,2n -k3,3)

if [ "${#files[@]}" -eq 0 ]; then
  fail "no UNBNK_*_DG_<date>_DT_<date>.json file found in $SAMPLES_DIR"
  exit 1
fi

log "Uploading ${#files[@]} file(s) from $SAMPLES_DIR to s3://$BUCKET/"
failed=0
for path in "${files[@]}"; do
  name="$(basename "$path")"
  # A plain PutObject: it is the event the Lambda trigger listens to
  if aws s3api put-object --bucket "$BUCKET" --key "$name" --body "$path" \
    --content-type application/json > /dev/null; then
    log "  uploaded  $name"
  else
    fail "could not upload $name"
    failed=$((failed + 1))
  fi
done

if [ "$failed" -gt 0 ]; then
  fail "$failed of ${#files[@]} file(s) failed"
  exit 1
fi
log "All ${#files[@]} file(s) uploaded"
