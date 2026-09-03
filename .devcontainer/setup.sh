#!/usr/bin/env bash
# Runs once when the Codespace is created. Installs everything needed to
# demo the paper-trading dashboard — no database required (the dashboard
# degrades gracefully without one; paper positions live in-memory + a
# JSON file, not Postgres).
set -e
cd "$(dirname "$0")/.."

python3 -m venv .venv
.venv/bin/pip install --no-input -q --upgrade pip
grep -v "^pandas-ta>" requirements.txt > /tmp/req.txt || cp requirements.txt /tmp/req.txt
.venv/bin/pip install --no-input -q -r /tmp/req.txt
.venv/bin/pip install --no-input -q pandas-ta-classic
SITE=$(.venv/bin/python -c "import site; print(site.getsitepackages()[0])")
echo "from pandas_ta_classic import *" > "$SITE/pandas_ta.py"
echo "from pandas_ta_classic import __version__" >> "$SITE/pandas_ta.py"

[ -f .env ] || cp .env.example .env
# Paper mode needs no broker credentials — TRADE_MODE defaults to "paper".
# DB_* vars point at a Postgres that doesn't exist here; that's fine, the
# app logs a warning and keeps running with db_connected=false.

cd dashboard
npm install --no-audit --no-fund
