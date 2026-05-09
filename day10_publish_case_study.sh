#!/usr/bin/env bash
# day10_publish_case_study.sh
# Day 10 — validate pipeline state, sync Gold → Superset, commit & push case study.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

# Probe Homebrew Java paths then fall back to java_home
for _jpath in \
    /opt/homebrew/opt/openjdk@17 \
    /usr/local/opt/openjdk@17 \
    /opt/homebrew/opt/openjdk@21 \
    /usr/local/opt/openjdk@21; do
    if [[ -x "$_jpath/bin/java" ]]; then
        export JAVA_HOME="$_jpath"
        export PATH="$JAVA_HOME/bin:$PATH"
        break
    fi
done
if [[ -z "${JAVA_HOME:-}" ]]; then
    _jh="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
    if [[ -n "$_jh" ]]; then
        export JAVA_HOME="$_jh"
        export PATH="$JAVA_HOME/bin:$PATH"
    fi
fi

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

ok()   { echo -e "${GREEN}  ✓${NC} $*"; }
warn() { echo -e "${YELLOW}  ⚠${NC} $*"; }
fail() { echo -e "${RED}  ✗${NC} $*"; }
hdr()  { echo -e "\n${BOLD}$*${NC}"; }

# ── 0. Banner ────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Contract-Driven Platform — Day 10: Publish Case Study  ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── 1. Pre-flight checks ─────────────────────────────────────────────────────
hdr "Step 1/5 — Pre-flight checks"

if ! java -version &>/dev/null; then
    fail "Java not found. Install with: brew install openjdk@17"
    exit 1
fi
ok "Java: $(java -version 2>&1 | head -1)"

if ! python -c "import pyspark" 2>/dev/null; then
    fail "pyspark not importable. Check your venv/conda environment."
    exit 1
fi
ok "PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"

DELTA_LOCAL="${LOCAL_DELTA_PATH:-/tmp/contract-driven-platform}"
GOLD_PATH="$DELTA_LOCAL/delta/gold.db"

if [[ -d "$GOLD_PATH" ]]; then
    ok "Gold Delta tables found at $GOLD_PATH"
    GOLD_EXISTS=true
else
    warn "Gold Delta tables not found at $GOLD_PATH"
    warn "Skipping Superset sync. Run the full pipeline first:"
    warn "  python flink/flink_consumer.py   # ingest from Kafka"
    warn "  dbt run                          # Bronze → Silver → Gold"
    GOLD_EXISTS=false
fi

# ── 2. Sync Gold → Superset (if tables exist) ────────────────────────────────
hdr "Step 2/5 — Sync Gold Delta → Superset SQLite"

if [[ "$GOLD_EXISTS" == "true" ]]; then
    python observability/dashboards/sync_gold_to_superset.py
    ok "gold_data.db updated — Superset will show real pipeline data"
else
    warn "Skipped (no Gold Delta tables)"
fi

# ── 3. Verify docs ────────────────────────────────────────────────────────────
hdr "Step 3/5 — Verify docs"

if [[ -f "docs/case_study.md" ]]; then
    WORD_COUNT=$(wc -w < docs/case_study.md)
    ok "docs/case_study.md exists ($WORD_COUNT words)"
else
    fail "docs/case_study.md missing"
    exit 1
fi

if [[ -f "README.md" ]]; then
    ok "README.md exists"
fi

SCREENSHOTS=(
    "observability/dashboards/screenshots/chart_1_Daily_Gross_Revenue_by_Currency.png"
    "observability/dashboards/screenshots/chart_2_Daily_Order_Count.png"
    "observability/dashboards/screenshots/chart_3_Payment_Success_Rate_by_Method.png"
    "observability/dashboards/screenshots/chart_4_Contract_Violations_by_Topic_and_Error_Type.png"
    "observability/dashboards/screenshots/chart_5_DLQ_Error_Type_Breakdown.png"
    "observability/dashboards/screenshots/dashboard_pipeline_health.png"
)
MISSING_SCREENSHOTS=0
for s in "${SCREENSHOTS[@]}"; do
    if [[ -f "$s" ]]; then
        ok "$(basename "$s")"
    else
        warn "Missing screenshot: $s"
        MISSING_SCREENSHOTS=$((MISSING_SCREENSHOTS + 1))
    fi
done
if [[ $MISSING_SCREENSHOTS -gt 0 ]]; then
    warn "$MISSING_SCREENSHOTS screenshot(s) missing — re-run Day 8 capture if needed"
fi

# ── 4. Git commit ─────────────────────────────────────────────────────────────
hdr "Step 4/5 — Git commit"

git add \
    docs/case_study.md \
    day10_publish_case_study.sh \
    observability/dashboards/sync_gold_to_superset.py \
    observability/dashboards/gold_data.db 2>/dev/null || true

STAGED=$(git diff --cached --name-only)
if [[ -z "$STAGED" ]]; then
    warn "Nothing new to commit (all files already tracked)"
else
    COMMIT_MSG="feat(day10): publish case study and sync real Gold data to Superset

- docs/case_study.md: full 10-day engineering case study
- sync_gold_to_superset.py: reads Gold Delta tables -> overwrites gold_data.db
- day10_publish_case_study.sh: end-to-end Day 10 publish script

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
    git commit -m "$COMMIT_MSG"
    ok "Committed: $(git log --oneline -1)"
fi

# ── 5. Push ───────────────────────────────────────────────────────────────────
hdr "Step 5/5 — Push to GitHub"

REMOTE=$(git remote get-url origin 2>/dev/null || true)
if [[ -z "$REMOTE" ]]; then
    warn "No git remote configured. Add one with:"
    warn "  git remote add origin https://github.com/arcofiero/contract-driven-platform.git"
    warn "  git push -u origin main"
else
    ok "Remote: $REMOTE"
    git push origin main
    ok "Pushed to origin/main"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║                   Day 10 Complete                        ║${NC}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════╣${NC}"
echo -e "${BOLD}║  Case study   → docs/case_study.md                       ║${NC}"
echo -e "${BOLD}║  Superset     → http://localhost:8088  (real data)        ║${NC}"
echo -e "${BOLD}║  GitHub       → $REMOTE  ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
