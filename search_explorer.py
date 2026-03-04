#!/usr/bin/env python

'''
MongoDB Atlas PoC — Interactive Search Explorer
=======================================
A visual, interactive demo of MongoDB Atlas Search capabilities.
Showcases fuzzy matching, address relaxation, cross-collection search,
and relevance scoring — designed to make stakeholders say "wow."

Usage:
    export MONGODB_URI="mongodb+srv://..."
    export MONGODB_DATABASE="poc_demo"
    python search_explorer.py

    Then open http://localhost:5060 in your browser.
'''

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime

import pymongo
from flask import Flask, Response, request, jsonify, render_template_string

from locust_db_config import resolve_config

app = Flask(__name__)

_CLIENT = None
_DB = None


def get_db():
    global _CLIENT, _DB
    if _DB is not None:
        return _DB
    cfg = resolve_config()
    if not cfg:
        return None
    _CLIENT = pymongo.MongoClient(cfg.connection_string, socketTimeoutMS=15000, connectTimeoutMS=5000)
    _DB = _CLIENT[cfg.database_name]
    return _DB


# ------------------------------------------------------------------ #
#  Search pipeline builders (mirrors locust_03_search.py)             #
# ------------------------------------------------------------------ #

def build_core_search(name, city='', state='', street='', limit=10):
    should = []
    if city:
        should.append({'text': {'query': city, 'path': 'current.address.city', 'score': {'boost': {'value': 2}}}})
    if state:
        should.append({'text': {'query': state, 'path': 'current.address.state', 'score': {'boost': {'value': 1}}}})
    if street:
        should.append({'text': {'query': street, 'path': 'current.address.line1', 'score': {'boost': {'value': 3}}}})

    if name:
        compound = {
            'must': [{'text': {'query': name, 'path': 'current.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}],
        }
        if should:
            compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'current.name'}}]}

    return [
        {'$search': {'index': 'core_search', 'compound': compound}},
        {'$limit': limit},
        {'$project': {
            'dunsNumber': 1,
            'current.name': 1,
            'current.address': 1,
            'employees.total': 1,
            'financials.latest.total_revenue': 1,
            'assessment.credit_score': 1,
            'legal.structure': 1,
            'legal.status': 1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


def build_trade_search(name, city='', state='', limit=10):
    should = []
    if city:
        should.append({'text': {'query': city, 'path': 'account.address.city', 'score': {'boost': {'value': 2}}}})
    if state:
        should.append({'text': {'query': state, 'path': 'account.address.state', 'score': {'boost': {'value': 1}}}})
    if name:
        compound = {'must': [{'text': {'query': name, 'path': 'account.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}]}
        if should:
            compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'account.name'}}]}

    return [
        {'$search': {'index': 'trade_search', 'compound': compound}},
        {'$limit': limit},
        {'$project': {
            'uuid': 1,
            'account.name': 1,
            'account.address': 1,
            'matched': 1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


def build_public_records_search(name, city='', state='', limit=10):
    should = []
    if city:
        should.append({'text': {'query': city, 'path': 'role_players.addresses.city', 'score': {'boost': {'value': 2}}}})
    if state:
        should.append({'text': {'query': state, 'path': 'role_players.addresses.state', 'score': {'boost': {'value': 1}}}})
    if name:
        compound = {'must': [{'text': {'query': name, 'path': 'role_players.names.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}]}
        if should:
            compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'role_players.names.name'}}]}

    return [
        {'$search': {'index': 'public_records_search', 'compound': compound}},
        {'$limit': limit},
        {'$project': {
            'filing_id': 1,
            'filing_type': 1,
            'matched': 1,
            'duns_numbers': 1,
            'role_players': 1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


# ------------------------------------------------------------------ #
#  Address relaxation ("always return something")                       #
# ------------------------------------------------------------------ #

def relaxed_search(db, collection, name, street='', city='', state='', limit=10):
    '''Progressive address relaxation: street+city+state → city+state → state → name only.'''
    coll = db[collection]
    attempts = [
        {'street': street, 'city': city, 'state': state, 'level': 'Full address'},
        {'street': '',     'city': city, 'state': state, 'level': 'City + State'},
        {'street': '',     'city': '',   'state': state, 'level': 'State only'},
        {'street': '',     'city': '',   'state': '',    'level': 'Name only (national)'},
    ]

    if not street:
        attempts = attempts[1:]

    results_by_level = []
    for attempt in attempts:
        pipeline = build_core_search(name, attempt['city'], attempt['state'], attempt['street'], limit)
        try:
            results = list(coll.aggregate(pipeline))
        except Exception:
            results = []
        results_by_level.append({
            'level': attempt['level'],
            'count': len(results),
            'results': results,
        })
        if results:
            break

    return results_by_level


# ------------------------------------------------------------------ #
#  Typo generator for the fuzzy search demo                           #
# ------------------------------------------------------------------ #

TYPO_OPS = [
    ('swap', 'Swapped two adjacent letters'),
    ('drop', 'Dropped a letter'),
    ('add', 'Added a random letter'),
    ('replace', 'Replaced a letter'),
]

def introduce_typo(text):
    if len(text) < 3:
        return text, 'Too short for typo'
    chars = list(text)
    op, desc = random.choice(TYPO_OPS)
    idx = random.randint(1, len(chars) - 2)

    if op == 'swap' and idx < len(chars) - 1:
        chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
    elif op == 'drop':
        chars.pop(idx)
    elif op == 'add':
        chars.insert(idx, random.choice('abcdefghijklmnopqrstuvwxyz'))
    elif op == 'replace':
        chars[idx] = random.choice('abcdefghijklmnopqrstuvwxyz')

    return ''.join(chars), desc


# ------------------------------------------------------------------ #
#  API routes                                                          #
# ------------------------------------------------------------------ #

@app.route('/api/search', methods=['POST'])
def api_search():
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    body = request.json or {}
    name = (body.get('name') or '').strip()
    city = (body.get('city') or '').strip()
    state = (body.get('state') or '').strip()
    street = (body.get('street') or '').strip()
    search_type = body.get('type', 'core')
    limit = min(int(body.get('limit', 10)), 25)

    if not name and not city and not state and not street:
        return jsonify({'error': 'Enter at least one search term (name, city, state, or street)'}), 400

    is_duns = name and name.isdigit() and len(name) == 9
    if is_duns:
        tic = time.time()
        pr_collections = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']
        results = []
        sources_checked = ['duns']
        doc = db['duns'].find_one({'dunsNumber': name})
        if doc:
            doc['_id'] = str(doc['_id'])
            doc['score'] = 1.0
            doc['_source_collection'] = 'duns'
            results.append(doc)
        for cn in pr_collections:
            sources_checked.append(cn)
            for pr_doc in db[cn].find({'role_players.duns_number': name}).limit(5):
                pr_doc['_id'] = str(pr_doc['_id'])
                pr_doc['score'] = 0.9
                pr_doc['_source_collection'] = cn
                results.append(pr_doc)
        elapsed = (time.time() - tic) * 1000
        return jsonify({'results': results, 'count': len(results), 'elapsed_ms': round(elapsed, 1),
                        'query': {'name': name, 'city': city, 'state': state, 'street': street},
                        'type': 'duns_lookup', 'duns_lookup': True,
                        'pipeline': [{'$match': {'dunsNumber': name}}, {'note': f'searched {", ".join(sources_checked)}'}]})

    tic = time.time()
    try:
        if search_type == 'core':
            pipeline = build_core_search(name, city, state, street, limit)
            results = list(db['duns'].aggregate(pipeline))
        elif search_type == 'trade':
            pipeline = build_trade_search(name, city, state, limit)
            results = list(db['entity_trade'].aggregate(pipeline))
        elif search_type == 'public_records':
            pr_collections = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']
            results = []
            for coll_name in pr_collections:
                pipeline = build_public_records_search(name, city, state, limit)
                try:
                    r = list(db[coll_name].aggregate(pipeline))
                    for doc in r:
                        doc['_source_collection'] = coll_name
                    results.extend(r)
                except Exception:
                    pass
            results.sort(key=lambda x: x.get('score', 0), reverse=True)
            results = results[:limit]
        else:
            return jsonify({'error': f'Unknown search type: {search_type}'}), 400

        elapsed = (time.time() - tic) * 1000

        for r in results:
            if '_id' in r:
                r['_id'] = str(r['_id'])

        return jsonify({
            'results': results,
            'count': len(results),
            'elapsed_ms': round(elapsed, 1),
            'query': {'name': name, 'city': city, 'state': state, 'street': street},
            'type': search_type,
            'pipeline': json.loads(json.dumps(pipeline, default=str)),
        })

    except pymongo.errors.OperationFailure as e:
        elapsed = (time.time() - tic) * 1000
        err_str = str(e)
        if 'index not found' in err_str.lower() or 'search index' in err_str.lower():
            return jsonify({
                'error': 'Atlas Search index not found. Please create the required search indexes.',
                'detail': err_str,
                'elapsed_ms': round(elapsed, 1),
            }), 400
        return jsonify({'error': err_str, 'elapsed_ms': round(elapsed, 1)}), 500
    except Exception as e:
        elapsed = (time.time() - tic) * 1000
        return jsonify({'error': str(e), 'elapsed_ms': round(elapsed, 1)}), 500


@app.route('/api/relaxation', methods=['POST'])
def api_relaxation():
    '''Demo address relaxation — shows how search progressively widens.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    body = request.json or {}
    name = (body.get('name') or '').strip()
    city = (body.get('city') or '').strip()
    state = (body.get('state') or '').strip()
    street = (body.get('street') or '').strip()
    limit = min(int(body.get('limit', 10)), 25)

    if not name:
        return jsonify({'error': 'Name is required'}), 400

    tic = time.time()
    try:
        levels = relaxed_search(db, 'duns', name, street, city, state, limit)
        elapsed = (time.time() - tic) * 1000

        for level in levels:
            for r in level['results']:
                if '_id' in r:
                    r['_id'] = str(r['_id'])

        return jsonify({
            'levels': levels,
            'elapsed_ms': round(elapsed, 1),
        })
    except Exception as e:
        elapsed = (time.time() - tic) * 1000
        return jsonify({'error': str(e), 'elapsed_ms': round(elapsed, 1)}), 500


@app.route('/api/typo-demo', methods=['POST'])
def api_typo_demo():
    '''Introduce a typo and search again — proving fuzzy match resilience.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    body = request.json or {}
    name = (body.get('name') or '').strip()
    limit = min(int(body.get('limit', 10)), 25)

    if not name:
        return jsonify({'error': 'Name is required'}), 400

    typo_name, typo_desc = introduce_typo(name)

    tic = time.time()
    try:
        original_pipeline = build_core_search(name, limit=limit)
        typo_pipeline = build_core_search(typo_name, limit=limit)

        original_results = list(db['duns'].aggregate(original_pipeline))
        typo_results = list(db['duns'].aggregate(typo_pipeline))
        elapsed = (time.time() - tic) * 1000

        for r in original_results + typo_results:
            if '_id' in r:
                r['_id'] = str(r['_id'])

        original_duns = {r.get('dunsNumber') for r in original_results}
        typo_duns = {r.get('dunsNumber') for r in typo_results}
        overlap = original_duns & typo_duns

        return jsonify({
            'original': {'name': name, 'results': original_results, 'count': len(original_results)},
            'typo': {'name': typo_name, 'description': typo_desc, 'results': typo_results, 'count': len(typo_results)},
            'overlap_count': len(overlap),
            'overlap_pct': round(len(overlap) / max(len(original_duns), 1) * 100, 1),
            'elapsed_ms': round(elapsed, 1),
        })
    except Exception as e:
        elapsed = (time.time() - tic) * 1000
        return jsonify({'error': str(e), 'elapsed_ms': round(elapsed, 1)}), 500


@app.route('/api/sample-names', methods=['GET'])
def api_sample_names():
    '''Fetch a few random company names from the DB for quick demo searches.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    try:
        results = list(db['duns'].aggregate([
            {'$sample': {'size': 8}},
            {'$project': {'current.name': 1, 'current.address.city': 1, 'current.address.state': 1, 'dunsNumber': 1, '_id': 0}},
        ]))
        return jsonify({'samples': results})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/document/<duns_number>')
def api_document(duns_number):
    '''Fetch a full document by DUNS number (AP-2 point lookup).'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    tic = time.time()
    try:
        doc = db['duns'].find_one({'dunsNumber': duns_number})
        elapsed = (time.time() - tic) * 1000
        if not doc:
            return jsonify({'error': 'Not found', 'elapsed_ms': round(elapsed, 1)}), 404
        doc['_id'] = str(doc['_id'])
        for key in ('meta', 'financials', 'assessment'):
            if key in doc:
                doc[key] = json.loads(json.dumps(doc[key], default=str))
        return jsonify({'document': doc, 'elapsed_ms': round(elapsed, 1)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats')
def api_stats():
    '''Quick collection stats for the dashboard header.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500
    try:
        core_count = db['duns'].estimated_document_count()
        trade_count = db['entity_trade'].estimated_document_count()
        pr_count = sum(db[c].estimated_document_count() for c in ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies'])
        audit_count = db['duns_audit'].estimated_document_count()
        return jsonify({
            'core': core_count,
            'trade': trade_count,
            'public_records': pr_count,
            'audit': audit_count,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ------------------------------------------------------------------ #
#  UI                                                                  #
# ------------------------------------------------------------------ #

EXPLORER_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MongoDB Atlas PoC — Atlas Search Explorer</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --border: #30363d;
  --text: #c9d1d9; --text2: #8b949e; --text3: #484f58;
  --blue: #58a6ff; --green: #3fb950; --yellow: #d29922;
  --purple: #d2a8ff; --orange: #f0883e; --red: #f85149;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); }

.header {
  background: linear-gradient(135deg, var(--bg2) 0%, var(--bg) 100%);
  border-bottom: 1px solid var(--border); padding: 16px 32px;
  display: flex; align-items: center; gap: 16px;
}
.header h1 { font-size: 20px; font-weight: 600; color: #e6edf3; }
.header .subtitle { font-size: 13px; color: var(--text2); }
.header .stats { margin-left: auto; display: flex; gap: 16px; font-size: 12px; color: var(--text2); }
.header .stats .s { text-align: center; }
.header .stats .sn { font-size: 18px; font-weight: 700; color: var(--blue); display: block; font-variant-numeric: tabular-nums; }

.main { display: flex; height: calc(100vh - 60px); }

.search-panel {
  width: 420px; border-right: 1px solid var(--border); padding: 20px;
  background: var(--bg2); overflow-y: auto; display: flex; flex-direction: column; gap: 16px;
}
.search-panel h2 { font-size: 14px; color: var(--text2); text-transform: uppercase; letter-spacing: 1px; }

.tabs { display: flex; gap: 0; border-radius: 6px; overflow: hidden; border: 1px solid var(--border); }
.tab {
  flex: 1; padding: 8px 4px; text-align: center; font-size: 12px; font-weight: 600;
  cursor: pointer; background: transparent; color: var(--text2); border: none;
  transition: all 0.15s; text-transform: uppercase; letter-spacing: 0.5px;
}
.tab:hover { background: rgba(255,255,255,0.03); }
.tab.active { background: var(--blue); color: var(--bg); }
.tab:not(:last-child) { border-right: 1px solid var(--border); }

.field-group { display: flex; flex-direction: column; gap: 4px; }
.field-group label { font-size: 11px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px; }
.field-group input {
  padding: 10px 12px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--bg); color: var(--text); font-size: 14px; outline: none; transition: border 0.15s;
}
.field-group input:focus { border-color: var(--blue); }
.field-group input::placeholder { color: var(--text3); }

.address-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }

.btn-row { display: flex; gap: 8px; }
.btn {
  flex: 1; padding: 10px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
  cursor: pointer; border: none; transition: all 0.15s; text-transform: uppercase; letter-spacing: 0.5px;
}
.btn-primary { background: var(--blue); color: var(--bg); }
.btn-primary:hover { background: #79c0ff; }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-typo { background: var(--orange); color: var(--bg); }
.btn-typo:hover { background: #ffa657; }
.btn-relax { background: var(--purple); color: var(--bg); }
.btn-relax:hover { background: #e2c5ff; }

.quick-picks { display: flex; flex-wrap: wrap; gap: 6px; }
.quick-pick {
  padding: 4px 10px; border-radius: 12px; font-size: 11px;
  background: rgba(88,166,255,0.1); color: var(--blue); cursor: pointer;
  border: 1px solid transparent; transition: all 0.15s;
}
.quick-pick:hover { border-color: var(--blue); background: rgba(88,166,255,0.2); }

.results-panel { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }

.results-header {
  display: flex; align-items: center; gap: 12px; padding-bottom: 12px;
  border-bottom: 1px solid var(--border);
}
.results-header h2 { font-size: 16px; color: #e6edf3; }
.results-header .meta { margin-left: auto; font-size: 12px; color: var(--text2); display: flex; gap: 16px; }
.results-header .meta .elapsed { color: var(--green); font-weight: 600; }

.empty-results {
  flex: 1; display: flex; align-items: center; justify-content: center;
  color: var(--text3); font-size: 15px; flex-direction: column; gap: 8px;
}
.empty-results .hint { font-size: 12px; max-width: 300px; text-align: center; line-height: 1.6; }

.result-card {
  padding: 14px 16px; border-radius: 8px; border: 1px solid var(--border);
  background: var(--bg2); cursor: pointer; transition: all 0.15s;
  position: relative;
}
.result-card:hover { border-color: var(--blue); background: rgba(88,166,255,0.04); }
.result-card .rc-header { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
.result-card .rank { font-size: 11px; color: var(--text3); font-weight: 700; min-width: 24px; }
.result-card .company-name { font-size: 15px; font-weight: 600; color: #e6edf3; }
.result-card .duns-badge { font-size: 11px; font-family: monospace; color: var(--blue); background: rgba(88,166,255,0.1); padding: 2px 8px; border-radius: 4px; margin-left: auto; }
.result-card .rc-address { font-size: 12px; color: var(--text2); margin-bottom: 6px; }
.result-card .rc-meta { display: flex; gap: 16px; font-size: 11px; color: var(--text3); flex-wrap: wrap; }
.result-card .rc-meta span { display: flex; align-items: center; gap: 4px; }

.score-bar { display: inline-block; width: 60px; height: 6px; background: #21262d; border-radius: 3px; vertical-align: middle; overflow: hidden; }
.score-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

.rc-expand {
  display: none; margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border);
  font-size: 12px; font-family: monospace; white-space: pre-wrap; color: var(--text2);
  line-height: 1.6; max-height: 400px; overflow-y: auto;
}
.result-card.expanded .rc-expand { display: block; }

.pr-role-players { display: flex; flex-direction: column; gap: 6px; margin-top: 10px; }
.rp-pill {
  display: flex; align-items: center; gap: 8px; padding: 6px 10px;
  border-radius: 6px; font-size: 12px; border: 1px solid var(--border);
}
.rp-pill.rp-matched { background: rgba(63,185,80,0.05); border-color: rgba(63,185,80,0.2); }
.rp-pill.rp-unmatched { background: rgba(248,81,73,0.05); border-color: rgba(248,81,73,0.2); }
.rp-role { font-weight: 700; text-transform: uppercase; font-size: 10px; min-width: 80px; }
.rp-name { color: #e6edf3; font-weight: 600; }
.rp-addr { color: var(--text3); }
.rp-duns { margin-left: auto; font-family: monospace; font-size: 11px; color: var(--blue); background: rgba(88,166,255,0.1); padding: 1px 6px; border-radius: 3px; }
.rp-no-duns { margin-left: auto; font-size: 10px; color: #f85149; font-style: italic; }
.pr-filing-title { font-size: 14px; }

.typo-compare {
  border: 1px solid var(--border); border-radius: 8px; overflow: hidden;
}
.typo-row { display: grid; grid-template-columns: 1fr 1fr; }
.typo-col { padding: 12px 16px; }
.typo-col:first-child { border-right: 1px solid var(--border); }
.typo-col h4 { font-size: 12px; color: var(--text2); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
.typo-col .tq { font-size: 16px; font-weight: 600; margin-bottom: 8px; }
.typo-col .tq.original { color: var(--green); }
.typo-col .tq.typo { color: var(--orange); }
.typo-col .tq .typo-desc { font-size: 11px; font-weight: 400; color: var(--text3); margin-left: 8px; }
.typo-overlap { padding: 10px 16px; background: rgba(63,185,80,0.08); border-top: 1px solid var(--border); font-size: 13px; color: var(--green); font-weight: 600; text-align: center; }

.relax-level {
  border: 1px solid var(--border); border-radius: 8px; overflow: hidden; margin-bottom: 8px;
}
.relax-header {
  padding: 10px 16px; display: flex; align-items: center; gap: 10px;
  cursor: pointer; transition: background 0.15s;
}
.relax-header:hover { background: rgba(255,255,255,0.02); }
.relax-header .rl-label { font-size: 13px; font-weight: 600; }
.relax-header .rl-count { margin-left: auto; font-size: 12px; color: var(--text2); }
.relax-header .rl-badge {
  font-size: 10px; padding: 2px 8px; border-radius: 10px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.5px;
}
.rl-badge.found { background: rgba(63,185,80,0.2); color: var(--green); }
.rl-badge.empty { background: rgba(139,148,158,0.1); color: var(--text3); }
.rl-badge.skipped { background: rgba(139,148,158,0.05); color: var(--text3); }
.relax-body { display: none; padding: 0 16px 12px; }
.relax-level.open .relax-body { display: block; }

.error-box {
  padding: 12px 16px; border-radius: 8px; background: rgba(248,81,73,0.1);
  border: 1px solid rgba(248,81,73,0.3); color: var(--red); font-size: 13px;
}

.loading { text-align: center; padding: 40px; color: var(--text3); }
.loading::after { content: ''; display: inline-block; width: 16px; height: 16px; border: 2px solid var(--text3); border-top-color: var(--blue); border-radius: 50%; animation: spin 0.6s linear infinite; margin-left: 8px; vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }

.pipeline-toggle {
  font-size: 11px; color: var(--text3); cursor: pointer; text-decoration: underline;
}
.pipeline-toggle:hover { color: var(--text); }
.pipeline-box {
  display: none; margin-top: 8px; padding: 10px 12px; border-radius: 6px;
  background: var(--bg); border: 1px solid var(--border);
  font-family: monospace; font-size: 11px; white-space: pre-wrap; color: var(--text2);
  max-height: 200px; overflow-y: auto; line-height: 1.5;
}
.pipeline-box.visible { display: block; }
</style>
</head>
<body>
<div class="header">
  <h1>MongoDB Atlas PoC — Atlas Search Explorer</h1>
  <span class="subtitle">Fuzzy Matching &bull; Address Relaxation &bull; Cross-Collection</span>
  <div class="stats">
    <div class="s"><span class="sn" id="st-core">—</span>Core Entities</div>
    <div class="s"><span class="sn" id="st-trade">—</span>Trade Records</div>
    <div class="s"><span class="sn" id="st-pr">—</span>Public Records</div>
    <div class="s"><span class="sn" id="st-audit">—</span>Audit Events</div>
  </div>
</div>

<div class="main">
  <div class="search-panel">
    <h2>Search Type</h2>
    <div class="tabs">
      <button class="tab active" data-type="core" onclick="setType(this)">Core Entity</button>
      <button class="tab" data-type="trade" onclick="setType(this)">Trade</button>
      <button class="tab" data-type="public_records" onclick="setType(this)">Public Records</button>
    </div>

    <div class="field-group">
      <label>Company / Entity Name (optional)</label>
      <input type="text" id="q-name" placeholder="e.g. International Business Machines, 9-digit DUNS number, or search by city/state" autofocus>
    </div>

    <div class="field-group" id="street-group">
      <label>Street Address (optional)</label>
      <input type="text" id="q-street" placeholder="e.g. 1 New Orchard Road">
    </div>
    <div class="address-row">
      <div class="field-group">
        <label>City</label>
        <input type="text" id="q-city" placeholder="e.g. Armonk">
      </div>
      <div class="field-group">
        <label>State</label>
        <input type="text" id="q-state" placeholder="e.g. NY">
      </div>
    </div>

    <div class="btn-row">
      <button class="btn btn-primary" id="btn-search" onclick="doSearch()">Search</button>
    </div>
    <div class="btn-row">
      <button class="btn btn-typo" onclick="doTypoDemo()" title="Introduce a random typo and compare results">Typo Demo</button>
      <button class="btn btn-relax" onclick="doRelaxation()" title="Show address relaxation levels">Relaxation Demo</button>
    </div>

    <h2>Quick Picks</h2>
    <div class="quick-picks" id="quick-picks">
      <span class="quick-pick" onclick="loadSamples()">Load from database...</span>
    </div>

    <div id="pipeline-area" style="margin-top:auto;">
      <span class="pipeline-toggle" onclick="togglePipeline()">Show aggregation pipeline</span>
      <div class="pipeline-box" id="pipeline-box"></div>
    </div>
  </div>

  <div class="results-panel" id="results-panel">
    <div class="empty-results" id="empty-results">
      <div style="font-size:32px;opacity:0.3;">&#x1F50D;</div>
      <div>Search the seeded data</div>
      <div class="hint">
        Try a company name with fuzzy matching, use the <strong>Typo Demo</strong> to see how Atlas Search handles misspellings,
        or run <strong>Relaxation Demo</strong> to see progressive address widening.
      </div>
    </div>
  </div>
</div>

<script>
let searchType = 'core';
const nameInput = document.getElementById('q-name');
const cityInput = document.getElementById('q-city');
const stateInput = document.getElementById('q-state');
const streetInput = document.getElementById('q-street');
const resultsPanel = document.getElementById('results-panel');

nameInput.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });

fetch('/api/stats').then(r => r.json()).then(d => {
  if (!d.error) {
    document.getElementById('st-core').textContent = (d.core || 0).toLocaleString();
    document.getElementById('st-trade').textContent = (d.trade || 0).toLocaleString();
    document.getElementById('st-pr').textContent = (d.public_records || 0).toLocaleString();
    document.getElementById('st-audit').textContent = (d.audit || 0).toLocaleString();
  }
});

function setType(btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  searchType = btn.dataset.type;
  document.getElementById('street-group').style.display = searchType === 'core' ? '' : 'none';
}

function loadSamples() {
  fetch('/api/sample-names').then(r => r.json()).then(d => {
    const picks = document.getElementById('quick-picks');
    picks.innerHTML = '';
    (d.samples || []).forEach(s => {
      const name = s.current?.name || '?';
      const city = s.current?.address?.city || '';
      const state = s.current?.address?.state || '';
      const el = document.createElement('span');
      el.className = 'quick-pick';
      el.textContent = name.length > 25 ? name.slice(0, 25) + '...' : name;
      el.title = `${name}\n${city}, ${state}`;
      el.onclick = () => { nameInput.value = name; cityInput.value = city; stateInput.value = state; };
      picks.appendChild(el);
    });
    const refresh = document.createElement('span');
    refresh.className = 'quick-pick';
    refresh.textContent = 'Refresh...';
    refresh.onclick = loadSamples;
    picks.appendChild(refresh);
  });
}
loadSamples();

function showPipeline(pipeline) {
  const box = document.getElementById('pipeline-box');
  box.textContent = JSON.stringify(pipeline, null, 2);
}

function togglePipeline() {
  document.getElementById('pipeline-box').classList.toggle('visible');
}

function getQuery() {
  return { name: nameInput.value.trim(), city: cityInput.value.trim(), state: stateInput.value.trim(), street: streetInput.value.trim(), type: searchType, limit: 10 };
}

function scoreColor(score, max) {
  const pct = score / max;
  if (pct > 0.7) return '#3fb950';
  if (pct > 0.4) return '#d29922';
  return '#f85149';
}

function renderCoreCard(r, idx, maxScore) {
  const name = r.current?.name || '?';
  const addr = r.current?.address || {};
  const addrStr = [addr.line1, addr.city, addr.state, addr.zip].filter(Boolean).join(', ');
  const score = r.score || 0;
  const pct = maxScore > 0 ? (score / maxScore * 100) : 0;
  const revenue = r.financials?.latest?.total_revenue;
  const credit = r.assessment?.credit_score;
  const emps = r.employees?.total;
  const structure = r.legal?.structure || '';
  const status = r.legal?.status || '';

  return `<div class="result-card" onclick="this.classList.toggle('expanded')">
    <div class="rc-header">
      <span class="rank">#${idx + 1}</span>
      <span class="company-name">${esc(name)}</span>
      <span class="duns-badge">DUNS ${esc(r.dunsNumber || '?')}</span>
    </div>
    <div class="rc-address">${esc(addrStr)}</div>
    <div class="rc-meta">
      <span>Score: <strong style="color:${scoreColor(score, maxScore)}">${score.toFixed(2)}</strong>
        <span class="score-bar"><span class="score-fill" style="width:${pct}%;background:${scoreColor(score, maxScore)}"></span></span>
      </span>
      ${revenue ? `<span>Revenue: <strong>$${(revenue/1e6).toFixed(1)}M</strong></span>` : ''}
      ${credit ? `<span>Credit: <strong>${credit}</strong></span>` : ''}
      ${emps ? `<span>Employees: <strong>${emps.toLocaleString()}</strong></span>` : ''}
      ${structure ? `<span>${esc(structure)}</span>` : ''}
      ${status ? `<span style="color:${status==='active'?'#3fb950':'#f85149'}">${esc(status)}</span>` : ''}
    </div>
    <div class="rc-expand">${esc(JSON.stringify(r, null, 2))}</div>
  </div>`;
}

function renderTradeCard(r, idx, maxScore) {
  const name = r.account?.name || '?';
  const addr = r.account?.address || {};
  const addrStr = [addr.city, addr.state].filter(Boolean).join(', ');
  const score = r.score || 0;
  const pct = maxScore > 0 ? (score / maxScore * 100) : 0;
  const matched = r.matched;

  return `<div class="result-card" onclick="this.classList.toggle('expanded')">
    <div class="rc-header">
      <span class="rank">#${idx + 1}</span>
      <span class="company-name">${esc(name)}</span>
      <span class="duns-badge" style="background:${matched ? 'rgba(63,185,80,0.1)' : 'rgba(210,153,34,0.1)'};color:${matched ? '#3fb950' : '#d29922'}">${matched ? 'Matched' : 'Unmatched'}</span>
    </div>
    <div class="rc-address">${esc(addrStr)}</div>
    <div class="rc-meta">
      <span>Score: <strong style="color:${scoreColor(score, maxScore)}">${score.toFixed(2)}</strong>
        <span class="score-bar"><span class="score-fill" style="width:${pct}%;background:${scoreColor(score, maxScore)}"></span></span>
      </span>
    </div>
    <div class="rc-expand">${esc(JSON.stringify(r, null, 2))}</div>
  </div>`;
}

function renderPRCard(r, idx, maxScore) {
  const rolePlayers = r.role_players || [];
  const src = r._source_collection || r.filing_type || '?';
  const score = r.score || 0;
  const pct = maxScore > 0 ? (score / maxScore * 100) : 0;
  const amount = r.amount;
  const statusColor = {'open':'#3fb950','pending':'#d29922','closed':'#8b949e','appealed':'#f0883e'}[r.filing_status] || '#8b949e';

  let rpHtml = '';
  rolePlayers.forEach(rp => {
    const rpName = rp.names?.[0]?.name || 'Unknown';
    const rpAddr = rp.addresses?.[0] || {};
    const rpAddrStr = [rpAddr.city, rpAddr.state].filter(Boolean).join(', ');
    const hasDuns = !!rp.duns_number;
    const polColor = {'positive':'#3fb950','negative':'#f85149','neutral':'#8b949e'}[rp.polarity] || '#8b949e';
    rpHtml += `<div class="rp-pill${hasDuns ? ' rp-matched' : ' rp-unmatched'}">
      <span class="rp-role" style="color:${polColor}">${esc(rp.role_type || '?')}</span>
      <span class="rp-name">${esc(rpName)}</span>
      ${rpAddrStr ? `<span class="rp-addr">${esc(rpAddrStr)}</span>` : ''}
      ${hasDuns ? `<span class="rp-duns">${esc(rp.duns_number)}</span>` : '<span class="rp-no-duns">unmatched</span>'}
    </div>`;
  });

  return `<div class="result-card pr-card" onclick="this.classList.toggle('expanded')">
    <div class="rc-header">
      <span class="rank">#${idx + 1}</span>
      <span class="company-name pr-filing-title">${esc(src.toUpperCase())} — ${esc(r.case_number || r.filing_id || '?')}</span>
      <span class="duns-badge" style="background:rgba(210,168,255,0.1);color:#d2a8ff">${esc(src)}</span>
    </div>
    <div class="rc-address">${esc(r.court || '')}${r.jurisdiction ? ' (' + esc(r.jurisdiction) + ')' : ''} &mdash; Filed: ${esc(r.filed_date || '?')}</div>
    <div class="rc-meta">
      <span>Score: <strong style="color:${scoreColor(score, maxScore)}">${score.toFixed(2)}</strong>
        <span class="score-bar"><span class="score-fill" style="width:${pct}%;background:${scoreColor(score, maxScore)}"></span></span>
      </span>
      ${amount ? `<span>Amount: <strong>$${Number(amount).toLocaleString()}</strong></span>` : ''}
      <span>Status: <strong style="color:${statusColor}">${esc(r.filing_status || '?')}</strong></span>
      <span>Role Players: <strong>${rolePlayers.length}</strong></span>
    </div>
    <div class="pr-role-players">${rpHtml}</div>
    <div class="rc-expand">${esc(JSON.stringify(r, null, 2))}</div>
  </div>`;
}

function renderCard(r, idx, maxScore, type) {
  if (type === 'trade') return renderTradeCard(r, idx, maxScore);
  if (type === 'public_records') return renderPRCard(r, idx, maxScore);
  if (type === 'duns_lookup') {
    const src = r._source_collection || '';
    if (src === 'duns') return renderCoreCard(r, idx, maxScore);
    if (src) return renderPRCard(r, idx, maxScore);
  }
  return renderCoreCard(r, idx, maxScore);
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function doSearch() {
  const q = getQuery();
  if (!q.name && !q.city && !q.state && !q.street) { nameInput.focus(); return; }
  resultsPanel.innerHTML = '<div class="loading">Searching</div>';
  document.getElementById('btn-search').disabled = true;

  fetch('/api/search', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(q) })
  .then(r => r.json())
  .then(d => {
    document.getElementById('btn-search').disabled = false;
    if (d.error) { resultsPanel.innerHTML = `<div class="error-box">${esc(d.error)}${d.detail ? '<br><br>' + esc(d.detail) : ''}</div>`; return; }

    if (d.pipeline) showPipeline(d.pipeline);
    const maxScore = d.results.length > 0 ? Math.max(...d.results.map(r => r.score || 0)) : 1;
    const lookupBadge = d.duns_lookup ? '<span style="background:#1f6feb;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;margin-left:8px;">DUNS LOOKUP</span>' : '';
    let html = `<div class="results-header"><h2>${d.count} result${d.count!==1?'s':''}${lookupBadge}</h2><div class="meta"><span class="elapsed">${d.elapsed_ms}ms</span><span>${esc(d.query.name)}${d.query.city ? ', ' + esc(d.query.city) : ''}${d.query.state ? ' ' + esc(d.query.state) : ''}</span></div></div>`;

    if (d.results.length === 0) {
      html += '<div class="empty-results"><div>No results found</div><div class="hint">Try the Relaxation Demo to see progressive address widening.</div></div>';
    } else {
      d.results.forEach((r, i) => { html += renderCard(r, i, maxScore, d.type); });
    }
    resultsPanel.innerHTML = html;
  })
  .catch(e => { document.getElementById('btn-search').disabled = false; resultsPanel.innerHTML = `<div class="error-box">${esc(e.message)}</div>`; });
}

function doTypoDemo() {
  const name = nameInput.value.trim();
  if (!name) { nameInput.focus(); return; }
  resultsPanel.innerHTML = '<div class="loading">Running typo comparison</div>';

  fetch('/api/typo-demo', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ name, limit: 5 }) })
  .then(r => r.json())
  .then(d => {
    if (d.error) { resultsPanel.innerHTML = `<div class="error-box">${esc(d.error)}</div>`; return; }

    const maxOrig = d.original.results.length > 0 ? Math.max(...d.original.results.map(r => r.score || 0)) : 1;
    const maxTypo = d.typo.results.length > 0 ? Math.max(...d.typo.results.map(r => r.score || 0)) : 1;

    let html = `<div class="results-header"><h2>Fuzzy Match — Typo Resilience Demo</h2><div class="meta"><span class="elapsed">${d.elapsed_ms}ms</span></div></div>`;
    html += `<div class="typo-compare">`;
    html += `<div class="typo-row"><div class="typo-col"><h4>Original Query</h4><div class="tq original">${esc(d.original.name)}</div>`;
    html += `<div style="font-size:12px;color:var(--text2);margin-bottom:8px;">${d.original.count} results</div>`;
    d.original.results.forEach((r, i) => { html += renderCoreCard(r, i, maxOrig); });
    html += `</div><div class="typo-col"><h4>With Typo</h4><div class="tq typo">${esc(d.typo.name)}<span class="typo-desc">${esc(d.typo.description)}</span></div>`;
    html += `<div style="font-size:12px;color:var(--text2);margin-bottom:8px;">${d.typo.count} results</div>`;
    d.typo.results.forEach((r, i) => { html += renderCoreCard(r, i, maxTypo); });
    html += `</div></div>`;
    html += `<div class="typo-overlap">${d.overlap_pct}% result overlap — Atlas Search found the same entities despite the typo (${d.overlap_count} matching DUNS numbers)</div>`;
    html += `</div>`;

    resultsPanel.innerHTML = html;
  })
  .catch(e => { resultsPanel.innerHTML = `<div class="error-box">${esc(e.message)}</div>`; });
}

function doRelaxation() {
  const q = getQuery();
  if (!q.name) { nameInput.focus(); return; }
  if (!q.city && !q.state) { q.city = 'New York'; q.state = 'NY'; cityInput.value = 'New York'; stateInput.value = 'NY'; }
  resultsPanel.innerHTML = '<div class="loading">Running address relaxation</div>';

  fetch('/api/relaxation', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(q) })
  .then(r => r.json())
  .then(d => {
    if (d.error) { resultsPanel.innerHTML = `<div class="error-box">${esc(d.error)}</div>`; return; }

    let html = `<div class="results-header"><h2>Address Relaxation Demo</h2><div class="meta"><span class="elapsed">${d.elapsed_ms}ms</span><span>"Always return something"</span></div></div>`;
    html += `<div style="font-size:13px;color:var(--text2);margin-bottom:12px;">MongoDB progressively widens the search when exact address matching finds nothing. The first level with results is used.</div>`;

    let foundLevel = false;
    d.levels.forEach((lv, li) => {
      const isWinner = !foundLevel && lv.count > 0;
      if (isWinner) foundLevel = true;
      const badge = isWinner ? 'found' : (foundLevel ? 'skipped' : (lv.count === 0 ? 'empty' : 'found'));
      const badgeText = isWinner ? 'USED' : (foundLevel ? 'SKIPPED' : (lv.count === 0 ? 'NO RESULTS' : `${lv.count} results`));

      html += `<div class="relax-level ${isWinner ? 'open' : ''}" onclick="this.classList.toggle('open')">`;
      html += `<div class="relax-header">`;
      html += `<span class="rl-label" style="color:${isWinner ? '#3fb950' : '#8b949e'}">${esc(lv.level)}</span>`;
      html += `<span class="rl-badge ${badge}">${badgeText}</span>`;
      html += `<span class="rl-count">${lv.count} result${lv.count !== 1 ? 's' : ''}</span>`;
      html += `</div>`;
      html += `<div class="relax-body">`;
      if (lv.results.length === 0) {
        html += `<div style="padding:8px 0;color:var(--text3);font-size:12px;">No matches at this level — relaxing further.</div>`;
      } else {
        const maxS = Math.max(...lv.results.map(r => r.score || 0));
        lv.results.forEach((r, i) => { html += renderCoreCard(r, i, maxS); });
      }
      html += `</div></div>`;
    });

    resultsPanel.innerHTML = html;
  })
  .catch(e => { resultsPanel.innerHTML = `<div class="error-box">${esc(e.message)}</div>`; });
}
</script>
</body>
</html>'''


@app.route('/')
def index():
    return render_template_string(EXPLORER_HTML)


def main():
    parser = argparse.ArgumentParser(description='MongoDB Atlas PoC — Interactive Search Explorer')
    parser.add_argument('--port', type=int, default=5060, help='Web server port (default: 5060)')
    parser.add_argument('--uri', help='MongoDB connection string (or set MONGODB_URI)')
    parser.add_argument('--db', help='Database name (or set MONGODB_DATABASE)')
    args = parser.parse_args()

    cfg = resolve_config()
    uri = args.uri or (cfg.connection_string if cfg else None)
    db_name = args.db or (cfg.database_name if cfg else None)

    if not uri or not db_name:
        print('Set MONGODB_URI and MONGODB_DATABASE env vars, or pass --uri and --db.')
        sys.exit(1)

    print(f'[search-explorer] Database: {db_name}')
    print(f'[search-explorer] Open http://localhost:{args.port} in your browser')
    app.run(host='0.0.0.0', port=args.port, threaded=True)


if __name__ == '__main__':
    main()
