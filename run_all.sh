#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

PYTHON_BIN="${PYTHON_BIN:-python}"
DAYS="${DAYS:-30}"
JOBUP_STOP_AFTER_SEEN="${JOBUP_STOP_AFTER_SEEN:-40}"
TALENT_STOP_AFTER_SEEN="${TALENT_STOP_AFTER_SEEN:-120}"
GE_STOP_AFTER_SEEN="${GE_STOP_AFTER_SEEN:-120}"
RETRY_ERRORS="${RETRY_ERRORS:-1}"
RUN_PIPELINES="${RUN_PIPELINES:-1}"

echo "[RUN] JobUp scraper"
"$PYTHON_BIN" jobup/jobup.py \
  --location "Genève" \
  --days "$DAYS" \
  --stop-after-seen "$JOBUP_STOP_AFTER_SEEN"

echo "[RUN] Talent scraper"
"$PYTHON_BIN" talent/talent.py \
  --location "Genève" \
  --days "$DAYS" \
  --stop-after-seen "$TALENT_STOP_AFTER_SEEN"

echo "[RUN] GE scraper"
"$PYTHON_BIN" ge/ge_ch_scraper.py \
  --days "$DAYS" \
  --stop-after-seen "$GE_STOP_AFTER_SEEN"

if [[ "$RUN_PIPELINES" == "1" ]]; then
  PIPE_FLAGS=()
  if [[ "$RETRY_ERRORS" == "1" ]]; then
    PIPE_FLAGS+=(--retry-errors)
  fi

  echo "[RUN] JobUp professions pipeline"
  "$PYTHON_BIN" jobup/professions_pipeline.py \
    --input data/jobup/professions.json \
    "${PIPE_FLAGS[@]}"

  echo "[RUN] Talent professions pipeline"
  "$PYTHON_BIN" talent/professions_pipeline.py \
    --input data/talent/professions.json \
    "${PIPE_FLAGS[@]}"

  echo "[RUN] GE professions pipeline"
  "$PYTHON_BIN" ge/professions_pipeline.py \
    --input data/ge/professions.json \
    "${PIPE_FLAGS[@]}"

fi

echo "[DONE] run_all finalizado."
