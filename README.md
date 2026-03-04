# MongoDB Atlas PoC — Demo Suite

An end-to-end proof-of-concept demonstrating MongoDB Atlas capabilities for entity resolution workloads:

- **Fuzzy Search** — Atlas Search with typo tolerance, address relaxation, and cross-collection queries
- **Change Detection** — Real-time change stream monitoring with publish/suppress event classification
- **Many-to-Many Relationships** — Interactive force-directed graph of entity ↔ public-record linkages
- **Load Testing** — Four Locust scripts for seeding data and benchmarking search/ops at scale

Everything runs from a single unified web hub.

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url> && cd <repo-name>

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

| File | Purpose |
|------|---------|
| `demo_hub.py` | Unified web hub — single entry point at port 5050 |
| `search_explorer.py` | Standalone Atlas Search Explorer |
| `change_stream_dashboard.py` | Standalone Change Stream Dashboard |
| `change_stream_listener.py` | CLI change stream monitor |
| `relationship_graph.py` | Standalone Relationship Graph |
| `locust_01_seed_core.py` | Seed core entity records |
| `locust_02_seed_public_records.py` | Seed public records (suits, liens, etc.) |
| `locust_03_search.py` | Atlas Search benchmark |
| `locust_04_ops.py` | Ops & change detection benchmark |
| `locust_db_config.py` | Shared Locust configuration module |
| `.env.example` | Environment variable template |
| `setup.sh` | One-command setup script |

---

## Atlas Search Indexes

The Search Explorer requires three Atlas Search indexes on your database. Create them in the Atlas UI or via the CLI:

### `core_search` (on `duns` collection)

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "businessName": { "type": "string", "analyzer": "lucene.standard" },
      "streetAddress": { "type": "string", "analyzer": "lucene.standard" },
      "city": { "type": "string", "analyzer": "lucene.standard" },
      "state": { "type": "string", "analyzer": "lucene.standard" },
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
      "businessName": { "type": "string", "analyzer": "lucene.standard" },
      "streetAddress": { "type": "string", "analyzer": "lucene.standard" },
      "city": { "type": "string", "analyzer": "lucene.standard" },
      "state": { "type": "string", "analyzer": "lucene.standard" }
    }
  }
}
```

### `public_records_search` (on each public record collection)

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "rolePlayers.name": { "type": "string", "analyzer": "lucene.standard" },
      "rolePlayers.address.street": { "type": "string", "analyzer": "lucene.standard" },
      "rolePlayers.address.city": { "type": "string", "analyzer": "lucene.standard" },
      "rolePlayers.address.state": { "type": "string", "analyzer": "lucene.standard" }
    }
  }
}
```

---

## Configuration

All configuration is via environment variables (or a `.env` file):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGODB_URI` | Yes | — | Atlas connection string |
| `MONGODB_DATABASE` | Yes | `poc_demo` | Database name |
| `MONGODB_COLLECTION` | No | `duns` | Primary collection |
| `LOCUST_BULK_SIZE` | No | `500` | Bulk insert batch size |
| `LOCUST_CANDIDATE_LIMIT` | No | `10` | Search result limit |
| `LOCUST_BULK_FILE_SIZE` | No | `50` | Ops bulk file size |

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
```

---

## License

Internal use — MongoDB Solutions Architecture.
