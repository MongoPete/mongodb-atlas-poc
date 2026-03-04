# MongoDB Atlas PoC — Demo Suite

An end-to-end proof-of-concept demonstrating MongoDB Atlas capabilities for entity resolution workloads:

- **Fuzzy Search** — Atlas Search with typo tolerance, address relaxation, cross-collection queries, and aggregation pipeline viewer
- **Change Detection** — Real-time change stream monitoring with publish/suppress classification, public records tracking, and cascading cross-collection updates
- **Many-to-Many Relationships** — Interactive D3.js force-directed graph of entity ↔ public-record linkages
- **Load Testing** — Four Locust scripts embedded in the hub with inline parameter controls (batch size, collection, etc.)

Everything runs from a single unified web hub at **http://localhost:5050**.

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone https://github.com/MongoPete/mongodb-atlas-poc.git
cd mongodb-atlas-poc

# 2. Run setup (creates venv, installs deps, prompts for connection string)
./setup.sh

# 3. Activate and launch
source .venv/bin/activate
python demo_hub.py
```

Open **http://localhost:5050** in your browser.

### Manual Setup

If you prefer to configure manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create .env with your connection details
cp .env.example .env
# Edit .env — set MONGODB_URI and MONGODB_DATABASE
```

---

## Requirements

- **Python 3.10+** (3.12 recommended)
- **MongoDB Atlas** cluster with the target database
- **Atlas Search indexes** (for the Search Explorer) — see below

---

## What's Included

### Demo Hub (single entry point)

| File | Purpose |
|------|---------|
| `demo_hub.py` | Unified web hub — all demos + Locust UIs on port 5050 |

### Demo Applications (also run standalone)

| File | Purpose |
|------|---------|
| `search_explorer.py` | Atlas Search Explorer — fuzzy search, typo demo, address relaxation, aggregation pipeline viewer |
| `change_stream_dashboard.py` | Change Stream Dashboard — real-time feed with filtering, reset, public records tracking |
| `change_stream_listener.py` | CLI change stream monitor (terminal output) |
| `relationship_graph.py` | Relationship Graph — interactive D3.js force-directed visualization |

### Load Testing

| File | Purpose |
|------|---------|
| `locust_01_seed_core.py` | Seed core entity records into `duns` collection |
| `locust_02_seed_public_records.py` | Seed public records (suits, liens, judgments, UCCs, bankruptcies) |
| `locust_03_search.py` | Atlas Search benchmark across all collections |
| `locust_04_ops.py` | Ops & change detection — agent updates, bulk file processing, publish/suppress |

### Configuration

| File | Purpose |
|------|---------|
| `locust_db_config.py` | Shared configuration module (env vars + `--host` fallback) |
| `.env.example` | Environment variable template |
| `setup.sh` | One-command setup script |
| `requirements.txt` | Python dependencies |

---

## Hub Features

### Search Explorer (`/search`)
- Fuzzy name + address search across core entities, trade accounts, and public records
- **Typo Demo** — introduces a random typo and compares results to show resilience
- **Address Relaxation Demo** — progressively widens search (full address → city+state → state → name only)
- **Aggregation Pipeline Viewer** — toggle to see the exact `$search` pipeline Atlas executes

### Change Stream Dashboard (`/change-stream`)
- Real-time SSE feed of all database changes
- **Publish/Suppress classification** — bulk file changes flagged as changed vs. unchanged
- **Public Records tracking** — inserts into suits/liens/etc. shown with filing details
- **Cascading updates** — name changes on core entities automatically propagate to linked role players in public records
- **Filtering** — by event type (publish, suppress, name, financial, score, bulk, public records, cascade)
- **Reset Feed** — clear all events and counters to start fresh

### Relationship Graph (`/graph`)
- Interactive force-directed graph of DUNS entities ↔ public record filings
- Click nodes to see entity details, filing metadata, and role player info
- Unmatched nodes highlighted for research agent investigation

### Locust Load Testing (`/locust/*`)
- All 4 Locust scripts accessible as embedded iframe tabs
- **Inline parameter controls** — adjust batch size, collection, result limits without restarting
- **Apply & Restart** — change settings and relaunch with one click
- Start/Stop controls with live status indicators

---

## Atlas Search Indexes

The Search Explorer requires three Atlas Search indexes. Create them in the Atlas UI or via CLI.

### `core_search` (on `duns` collection)

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "current.name": { "type": "string", "analyzer": "lucene.standard" },
      "current.address.line1": { "type": "string", "analyzer": "lucene.standard" },
      "current.address.city": { "type": "string", "analyzer": "lucene.standard" },
      "current.address.state": { "type": "string", "analyzer": "lucene.standard" },
      "dunsNumber": { "type": "string", "analyzer": "lucene.keyword" }
    }
  }
}
```

### `trade_search` (on `entity_trade` collection)

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "account.name": { "type": "string", "analyzer": "lucene.standard" },
      "account.address.city": { "type": "string", "analyzer": "lucene.standard" },
      "account.address.state": { "type": "string", "analyzer": "lucene.standard" }
    }
  }
}
```

### `public_records_search` (on each of: `suits`, `liens`, `judgments`, `uccs`, `bankruptcies`)

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "role_players.names.name": { "type": "string", "analyzer": "lucene.standard" },
      "role_players.addresses.city": { "type": "string", "analyzer": "lucene.standard" },
      "role_players.addresses.state": { "type": "string", "analyzer": "lucene.standard" }
    }
  }
}
```

---

## Configuration

All configuration is via environment variables (or a `.env` file, auto-loaded via `python-dotenv`):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGODB_URI` | Yes | — | Atlas connection string |
| `MONGODB_DATABASE` | Yes | `poc_demo` | Database name |
| `MONGODB_COLLECTION` | No | `duns` | Primary collection |
| `LOCUST_BULK_SIZE` | No | `500` | Bulk insert batch size (scripts 01, 02) |
| `LOCUST_CANDIDATE_LIMIT` | No | `10` | Search result limit (script 03) |
| `LOCUST_BULK_FILE_SIZE` | No | `50` | Ops bulk file size (script 04) |

Locust parameters can also be adjusted live from the hub's inline settings bar.

---

## Running Individual Components

Each component can also run standalone:

```bash
# Search Explorer on port 5070
python search_explorer.py --port 5070

# Change Stream Dashboard on port 5050
python change_stream_dashboard.py --port 5050

# Relationship Graph on port 5080
python relationship_graph.py --port 5080

# Change Stream CLI listener
python change_stream_listener.py --audit

# Individual Locust scripts
python -m locust -f locust_01_seed_core.py --web-port 8089
python -m locust -f locust_02_seed_public_records.py --web-port 8090
python -m locust -f locust_03_search.py --web-port 8091
python -m locust -f locust_04_ops.py --web-port 8092
```

---

## License

Internal use — MongoDB Solutions Architecture.
