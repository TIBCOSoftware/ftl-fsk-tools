#!/usr/bin/env bash
# End-to-end migration runner: dry-run first, then live migration.
#
# Usage:
#   export KAFKA_CLASSPATH="..."
#   bash demo/run-migration.sh [--output-dir ./kof-output] [--dry-run-only]

set -euo pipefail
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"  # the tool's root directory
KOF_OUTPUT_DIR="$SCRIPT_DIR/../kof-output"
LOG_FILE="$SCRIPT_DIR/migration.log"
DRY_RUN_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)    KOF_OUTPUT_DIR="$2"; shift 2 ;;
    --dry-run-only)  DRY_RUN_ONLY=true; shift ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

KOF_OUTPUT_DIR="$(cd "$KOF_OUTPUT_DIR" && pwd)"
CONFIG="$KOF_OUTPUT_DIR/kafka-to-kof.properties"

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: $CONFIG not found."
  echo "Run tibftlimportconfig against demo/kraft/server-*.properties to generate kof-output/:"
  echo "  tibftlimportconfig --output-dir ./kof-output --realm-name insurance-demo \\"
  echo "    demo/kraft/server-1.properties demo/kraft/server-2.properties demo/kraft/server-3.properties"
  exit 1
fi

if grep -v '^\s*#' "$CONFIG" | grep -q '<KOF-HOST-'; then
  echo "ERROR: $CONFIG still contains <KOF-HOST-N> placeholders in target.bootstrap.servers."
  echo "Replace them with the actual FSK pserver hostnames (or 'localhost' for the demo):"
  echo "  sed -i '' 's/<KOF-HOST-[0-9]*>/localhost/g' \"$CONFIG\""
  exit 1
fi

if [[ -z "${KAFKA_CLASSPATH:-}" ]]; then
  echo "ERROR: KAFKA_CLASSPATH is not set."
  echo "  export KAFKA_CLASSPATH=\"\$KAFKA_HOME/libs/*\""
  exit 1
fi

run_tool() {
  local extra_args=("$@")
  bash "$MIGRATION_DIR/run-apachekafka-to-fsk.sh" \
    --config "$CONFIG" \
    "${extra_args[@]}" \
    2>&1 | tee -a "$LOG_FILE"
}

echo "" >> "$LOG_FILE"
echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') =====" >> "$LOG_FILE"

# ── Dry run ───────────────────────────────────────────────────────────────────
echo "==> Step 1/2: Dry run (no data will be written to FSK)"
echo ""
run_tool --dry-run
echo ""

if $DRY_RUN_ONLY; then
  echo "==> Dry-run-only mode. Exiting."
  echo "    Full log: $LOG_FILE"
  exit 0
fi

# ── Confirm ───────────────────────────────────────────────────────────────────
echo "───────────────────────────────────────────────────────────────"
read -r -p "Proceed with live migration? [y/N] " CONFIRM
echo ""
if [[ "${CONFIRM,,}" != "y" ]]; then
  echo "Migration cancelled."
  exit 0
fi

# ── Live migration ────────────────────────────────────────────────────────────
echo "==> Step 2/2: Live migration"
echo ""
run_tool

echo ""
echo "✓ Migration complete. Full log: $LOG_FILE"
