#!/usr/bin/env bash
# Runs once when the Codespace is created. Installs everything needed to
# demo the dashboard: Python/Node deps, a local Postgres seeded with
# synthetic NIFTY-I candles (so charts/regime/scanning have something to
# read before a real TrueData/Angel One feed is connected), and system
# time pinned to IST (the app's market-hours checks assume the host clock
# is already IST — true on a dev's own machine, false on a UTC container).
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

sudo apt-get update -qq
sudo apt-get install -y -qq postgresql > /dev/null
sudo service postgresql start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';" > /dev/null
sudo -u postgres psql -c "CREATE DATABASE trading;" > /dev/null 2>&1 || true
PGPASSWORD=postgres psql -h localhost -U postgres -d trading -f database/schema.sql > /dev/null 2>&1 || true
.venv/bin/python scripts/seed_demo_candles.py --days 15

cd dashboard
npm install --no-audit --no-fund
