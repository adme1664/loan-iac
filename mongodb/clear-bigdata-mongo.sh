#!/usr/bin/env bash
set -euo pipefail

# The connection string defaults to the local bigdata-mongo container, built
# from iac/.env so the password stays out of this file. Export MONGODB_URI to
# target another server.
if [[ -z "${MONGODB_URI:-}" ]]; then
  ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.env"
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Set MONGODB_URI, or create $ENV_FILE with MONGO_USER, MONGO_PASSWORD and MONGO_PORT." >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  if [[ -z "${MONGO_USER:-}" || -z "${MONGO_PASSWORD:-}" ]]; then
    echo "MONGO_USER and MONGO_PASSWORD must be set in $ENV_FILE." >&2
    exit 1
  fi
  # Credentials are percent-encoded so special characters do not break the URI
  uri_encode() { jq -rn --arg value "$1" '$value | @uri'; }
  MONGODB_URI="mongodb://$(uri_encode "$MONGO_USER"):$(uri_encode "$MONGO_PASSWORD")@localhost:${MONGO_PORT:-27017}/?authSource=admin"
fi

if [[ ! -t 0 ]]; then
  echo "Refusing to run without an interactive confirmation." >&2
  exit 1
fi

printf 'This will delete every document in database "bigdata" and preserve its collections and indexes.\n'
read -r -p 'Type bigdata to continue: ' confirmation
if [[ "$confirmation" != "bigdata" ]]; then
  echo "Cancelled."
  exit 1
fi

mongosh "$MONGODB_URI" --quiet --eval '
const targetDatabase = db.getSiblingDB("bigdata");
const collections = targetDatabase
  .getCollectionInfos({ type: "collection" })
  .filter(({ name }) => !name.startsWith("system."));

for (const { name } of collections) {
  const result = targetDatabase.getCollection(name).deleteMany({});
  print(`${name}: deleted ${result.deletedCount} document(s)`);
}
'
