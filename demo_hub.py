#!/usr/bin/env python

'''
MongoDB Atlas PoC — Unified Demo Hub
============================
Single entry point for all three PoC demo applications:
  1. Atlas Search Explorer     (fuzzy search)
  2. Change Stream Dashboard   (change detection)
  3. Relationship Graph        (many-to-many)

Usage:
    export MONGODB_URI="mongodb+srv://..."
    export MONGODB_DATABASE="poc_demo"
    python demo_hub.py

    Then open http://localhost:5050 in your browser.
'''

import os
import sys
import json
import queue
import time
import random
import argparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import threading
from datetime import datetime

import pymongo
from flask import Flask, Response, request, jsonify, render_template_string

from locust_db_config import resolve_config

app = Flask(__name__)

# ------------------------------------------------------------------ #
#  Shared DB connection                                                #
# ------------------------------------------------------------------ #
_CLIENT = None
_DB = None
PR_COLLECTIONS = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']


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


# ================================================================== #
#  CHANGE STREAM ENGINE (from change_stream_dashboard.py)              #
# ================================================================== #

import collections

event_queue = queue.Queue(maxsize=500)
recent_events = collections.deque(maxlen=200)
recent_events_lock = threading.Lock()

cs_stats = {
    'total': 0, 'updates': 0, 'inserts': 0,
    'published': 0, 'suppressed': 0,
    'name_changes': 0, 'financial_updates': 0, 'score_updates': 0,
    'bulk_changes': 0, 'bulk_suppressed': 0,
    'cascades': 0,
    'start_time': None,
}
cs_stats_lock = threading.Lock()

PR_CASCADE_COLLECTIONS = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']


def _cascade_update(db, duns_number, updated_fields):
    """Propagate changes from a DUNS entity to linked role players in public records.

    Returns a dict with cascade details: count, affected collections, old/new values.
    With synthetic data the DUNS numbers won't overlap across collections,
    so on miss we link a random matched role player to this DUNS first.
    """
    new_name = updated_fields.get('current.name')
    if not new_name or not duns_number:
        return None

    result_info = {'count': 0, 'collections': [], 'old_name': None, 'new_name': new_name, 'filing_ids': []}

    for coll_name in PR_CASCADE_COLLECTIONS:
        try:
            before_docs = list(db[coll_name].find(
                {'role_players.duns_number': duns_number},
                {'filing_id': 1, 'role_players.$': 1},
            ).limit(3))
            if before_docs:
                for bd in before_docs:
                    rp = bd.get('role_players', [{}])[0]
                    old_bname = (rp.get('names') or [{}])[0].get('business_name')
                    if old_bname and not result_info['old_name']:
                        result_info['old_name'] = old_bname
                    fid = bd.get('filing_id', str(bd.get('_id', '')))
                    if fid:
                        result_info['filing_ids'].append(fid)

            res = db[coll_name].update_many(
                {'role_players.duns_number': duns_number},
                {'$set': {'role_players.$[rp].names.0.business_name': new_name}},
                array_filters=[{'rp.duns_number': duns_number}],
            )
            if res.modified_count > 0:
                result_info['count'] += res.modified_count
                result_info['collections'].append((coll_name, res.modified_count))
        except Exception:
            pass

    if result_info['count'] == 0:
        coll_name = random.choice(PR_CASCADE_COLLECTIONS)
        try:
            doc = db[coll_name].find_one(
                {'role_players.matched': True, 'role_players.duns_number': {'$ne': None}},
                {'_id': 1, 'filing_id': 1, 'role_players': 1},
            )
            if doc:
                rp = next((r for r in doc.get('role_players', []) if r.get('duns_number')), None)
                if rp:
                    old_bname = (rp.get('names') or [{}])[0].get('business_name', '(unknown)')
                    result_info['old_name'] = old_bname
                    old_duns = rp['duns_number']
                    db[coll_name].update_one(
                        {'_id': doc['_id'], 'role_players.duns_number': old_duns},
                        {'$set': {
                            'role_players.$.duns_number': duns_number,
                            'role_players.$.names.0.business_name': new_name,
                        }},
                    )
                    result_info['count'] = 1
                    result_info['collections'] = [(coll_name, 1)]
                    fid = doc.get('filing_id', str(doc.get('_id', '')))
                    if fid:
                        result_info['filing_ids'] = [fid]
        except Exception:
            pass

    return result_info if result_info['count'] > 0 else None


def _escape_html(s):
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def _bucket_tag(bucket):
    labels = {
        'name': ('NAME', 'bt-name'), 'financial': ('FINANCIAL', 'bt-financial'),
        'score': ('SCORE', 'bt-score'), 'bulk': ('BULK', 'bt-bulk'),
        'cascade': ('CASCADE', 'bt-cascade'),
    }
    if bucket in labels:
        text, cls = labels[bucket]
        return f'<span class="bucket-tag {cls}">{text}</span>'
    return ''


def _build_raw_detail(event):
    parts = {}
    op = event.get('operationType', '')
    parts['operation'] = op
    parts['collection'] = event.get('ns', {}).get('coll', '')
    dk = event.get('documentKey', {})
    if dk:
        parts['documentKey'] = str(dk.get('_id', ''))
    if op == 'insert' and 'fullDocument' in event:
        doc = event['fullDocument']
        for key in ('dunsNumber', 'action', 'publish_flag', 'change_details', 'current', 'assessment', 'financials'):
            if key in doc:
                val = doc[key]
                parts[key] = {k: str(v) for k, v in list(val.items())[:8]} if isinstance(val, dict) else str(val)
    if 'updateDescription' in event:
        ud = event['updateDescription'].get('updatedFields', {})
        if ud:
            parts['updatedFields'] = {k: str(v) for k, v in list(ud.items())[:10]}
    try:
        return json.dumps(parts, indent=2, default=str)
    except Exception:
        return '{}'


def format_event_html(event):
    op = event['operationType']
    ns_coll = event['ns']['coll']
    ts = event.get('clusterTime', '')
    doc_key = event.get('documentKey', {})
    doc_id = str(doc_key.get('_id', ''))
    is_audit = ns_coll.endswith('_audit')
    buckets = []
    raw_detail = _build_raw_detail(event)

    if is_audit and op == 'insert' and 'fullDocument' in event:
        doc = event['fullDocument']
        duns = _escape_html(doc.get('dunsNumber', '?'))
        action = _escape_html(doc.get('action', '?'))
        action_raw = doc.get('action', '')
        publish = doc.get('publish_flag')
        change = doc.get('change_details', {})
        with cs_stats_lock:
            if publish:
                cs_stats['published'] += 1
                if 'BULK_FILE' in action_raw: cs_stats['bulk_changes'] += 1
            else:
                cs_stats['suppressed'] += 1
                if 'BULK_FILE' in action_raw: cs_stats['bulk_suppressed'] += 1
            if 'NAME' in action_raw and 'BULK' not in action_raw: cs_stats['name_changes'] += 1
            elif 'FINANCIAL' in action_raw: cs_stats['financial_updates'] += 1
            elif 'SCORE' in action_raw: cs_stats['score_updates'] += 1
            elif 'BULK_FILE_NAME' in action_raw: cs_stats['name_changes'] += 1
        if publish:
            badge = '<span class="badge pub">PUBLISH</span>'; css_class = 'publish'; buckets.append('publish')
        else:
            badge = '<span class="badge sup">SUPPRESS</span>'; css_class = 'suppress'; buckets.append('suppress')
        if 'BULK_FILE' in action_raw or 'BULK' in action_raw: buckets.append('bulk')
        if 'NAME' in action_raw: buckets.append('name')
        if 'FINANCIAL' in action_raw: buckets.append('financial')
        if 'SCORE' in action_raw: buckets.append('score')
        bucket_tags = ''.join(_bucket_tag(b) for b in buckets if b not in ('publish', 'suppress'))
        detail_html = ''
        if change.get('old_name') and change.get('new_name'):
            detail_html = f'<div class="detail"><span class="old-val">{_escape_html(change["old_name"])}</span><span class="arrow">&rarr;</span><span class="new-val">{_escape_html(change["new_name"])}</span></div>'
        elif change.get('name_changed'):
            detail_html = f'<div class="detail"><span class="old-val">{_escape_html(change.get("old_name","?"))}</span><span class="arrow">&rarr;</span><span class="new-val">{_escape_html(change.get("new_name","?"))}</span></div>'
        html = (f'<div class="ev-header"><span class="ts">{ts}</span> {badge} {bucket_tags} <span class="coll">{ns_coll}</span> DUNS=<span class="duns">{duns}</span> <span class="action">{action}</span></div>{detail_html}<div class="expand-detail">{_escape_html(raw_detail)}</div>')
        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    if op == 'update':
        css_class = 'update'; badge = '<span class="badge upd">UPDATE</span>'; buckets.append('update')
        detail_html = ''
        if 'updateDescription' in event:
            fields = list(event['updateDescription'].get('updatedFields', {}).keys())
            ud = event['updateDescription'].get('updatedFields', {})
            if 'current.name' in ud:
                with cs_stats_lock: cs_stats['name_changes'] += 1
                buckets.append('name')
                detail_html = f'<div class="detail"><span class="new-val">name &rarr; {_escape_html(ud["current.name"])}</span></div>'
            elif 'financials.latest.total_revenue' in ud:
                buckets.append('financial')
                detail_html = f'<div class="detail">revenue &rarr; ${ud["financials.latest.total_revenue"]:,.0f}</div>'
            elif 'assessment.credit_score' in ud:
                buckets.append('score')
                detail_html = f'<div class="detail">credit_score &rarr; {_escape_html(ud["assessment.credit_score"])}</div>'
            short_fields = fields[:4]
            if short_fields:
                detail_html += f'<div class="fields">{", ".join(short_fields)}{"..." if len(fields) > 4 else ""}</div>'
        bucket_tags = ''.join(_bucket_tag(b) for b in buckets if b != 'update')
        duns_str = doc_id.replace('CORE-', '') if doc_id.startswith('CORE-') else doc_id
        html = f'<div class="ev-header"><span class="ts">{ts}</span> {badge} {bucket_tags} <span class="coll">{ns_coll}</span> DUNS=<span class="duns">{_escape_html(duns_str)}</span></div>{detail_html}<div class="expand-detail">{_escape_html(raw_detail)}</div>'
        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    if op == 'insert' and not is_audit:
        css_class = 'insert'; badge = '<span class="badge ins">INSERT</span>'; buckets.append('publish')
        detail_html = ''
        if 'fullDocument' in event:
            doc = event['fullDocument']
            name = _escape_html(doc.get('current', {}).get('name', ''))
            if name: detail_html = f'<div class="detail">{name}</div>'
        html = f'<div class="ev-header"><span class="ts">{ts}</span> {badge} <span class="coll">{ns_coll}</span> DUNS=<span class="duns">{_escape_html(doc_id)}</span></div>{detail_html}<div class="expand-detail">{_escape_html(raw_detail)}</div>'
        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    return None


def watch_changes(uri, db_name, collection_name):
    client = pymongo.MongoClient(uri, socketTimeoutMS=30000, connectTimeoutMS=5000)
    db = client[db_name]
    audit_name = f'{collection_name}_audit'
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'replace', 'delete']}, 'ns.coll': {'$in': [collection_name, audit_name]}}}]
    print(f'[change-stream] Watching {db_name}.{collection_name} + {audit_name}')
    with cs_stats_lock:
        cs_stats['start_time'] = datetime.utcnow().isoformat()
    stream = db.watch(pipeline, full_document='updateLookup')
    def _enqueue(evt):
        with recent_events_lock:
            recent_events.append(evt)
        try: event_queue.put_nowait(evt)
        except queue.Full:
            try: event_queue.get_nowait()
            except queue.Empty: pass
            event_queue.put_nowait(evt)

    try:
        for event in stream:
            with cs_stats_lock: cs_stats['total'] += 1
            formatted = format_event_html(event)
            if formatted:
                _enqueue(formatted)

            op = event.get('operationType', '')
            ns_coll = event.get('ns', {}).get('coll', '')
            if op == 'update' and ns_coll == collection_name:
                ud = event.get('updateDescription', {}).get('updatedFields', {})
                if 'current.name' in ud:
                    full_doc = event.get('fullDocument') or {}
                    duns_num = full_doc.get('dunsNumber', '')
                    new_name = ud['current.name']
                    if duns_num:
                        info = _cascade_update(db, duns_num, ud)
                        if info:
                            with cs_stats_lock: cs_stats['cascades'] += info['count']
                            ts_str = datetime.utcnow().strftime('%H:%M:%S')
                            coll_tags = ' '.join(
                                f'<span class="bucket-tag bt-cascade">{c}({n})</span>'
                                for c, n in info['collections']
                            )
                            diff_html = ''
                            if info.get('old_name'):
                                diff_html = (f'<div class="detail">'
                                             f'<span class="old-val">{_escape_html(info["old_name"])}</span>'
                                             f'<span class="arrow">&rarr;</span>'
                                             f'<span class="new-val">{_escape_html(info["new_name"])}</span>'
                                             f'</div>')
                            else:
                                diff_html = f'<div class="detail"><span class="new-val">{_escape_html(info["new_name"])}</span> propagated</div>'
                            filing_html = ''
                            if info.get('filing_ids'):
                                fids = ', '.join(str(f) for f in info['filing_ids'][:3])
                                extra = f' +{len(info["filing_ids"])-3} more' if len(info['filing_ids']) > 3 else ''
                                filing_html = f'<div class="fields">Filing IDs: {_escape_html(fids)}{extra}</div>'
                            cascade_evt = {
                                'html': (f'<div class="ev-header"><span class="ts">{ts_str}</span> '
                                         f'<span class="badge cascade">CASCADE</span> {coll_tags} '
                                         f'DUNS=<span class="duns">{_escape_html(duns_num)}</span> '
                                         f'&rarr; {info["count"]} role player{"s" if info["count"] != 1 else ""} updated</div>'
                                         f'{diff_html}{filing_html}'),
                                'css_class': 'cascade',
                                'buckets': ['cascade'],
                                'type': 'event',
                            }
                            _enqueue(cascade_evt)

            with cs_stats_lock: snap = dict(cs_stats)
            snap['type'] = 'stats'
            try: event_queue.put_nowait(snap)
            except queue.Full: pass
    except pymongo.errors.PyMongoError as e:
        print(f'[change-stream] Error: {e}')
    finally:
        stream.close(); client.close()


# ================================================================== #
#  SEARCH HELPERS (from search_explorer.py)                            #
# ================================================================== #

def build_core_search(name, city='', state='', street='', limit=10):
    should = []
    if city: should.append({'text': {'query': city, 'path': 'current.address.city', 'score': {'boost': {'value': 2}}}})
    if state: should.append({'text': {'query': state, 'path': 'current.address.state', 'score': {'boost': {'value': 1}}}})
    if street: should.append({'text': {'query': street, 'path': 'current.address.line1', 'score': {'boost': {'value': 3}}}})
    if name:
        compound = {'must': [{'text': {'query': name, 'path': 'current.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}]}
        if should: compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'current.name'}}]}
    return [{'$search': {'index': 'core_search', 'compound': compound}}, {'$limit': limit},
            {'$project': {'dunsNumber': 1, 'current.name': 1, 'current.address': 1, 'employees.total': 1,
                          'financials.latest.total_revenue': 1, 'assessment.credit_score': 1,
                          'legal.structure': 1, 'legal.status': 1, 'score': {'$meta': 'searchScore'}}}]

def build_trade_search(name, city='', state='', limit=10):
    should = []
    if city: should.append({'text': {'query': city, 'path': 'account.address.city', 'score': {'boost': {'value': 2}}}})
    if state: should.append({'text': {'query': state, 'path': 'account.address.state', 'score': {'boost': {'value': 1}}}})
    if name:
        compound = {'must': [{'text': {'query': name, 'path': 'account.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}]}
        if should: compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'account.name'}}]}
    return [{'$search': {'index': 'trade_search', 'compound': compound}}, {'$limit': limit},
            {'$project': {'uuid': 1, 'account.name': 1, 'account.address': 1, 'matched': 1, 'score': {'$meta': 'searchScore'}}}]

def build_public_records_search(name, city='', state='', limit=10):
    should = []
    if city: should.append({'text': {'query': city, 'path': 'role_players.addresses.city', 'score': {'boost': {'value': 2}}}})
    if state: should.append({'text': {'query': state, 'path': 'role_players.addresses.state', 'score': {'boost': {'value': 1}}}})
    if name:
        compound = {'must': [{'text': {'query': name, 'path': 'role_players.names.name', 'fuzzy': {'maxEdits': 2, 'prefixLength': 1}}}]}
        if should: compound['should'] = should
    else:
        compound = {'should': should} if should else {'must': [{'text': {'query': '*', 'path': 'role_players.names.name'}}]}
    return [{'$search': {'index': 'public_records_search', 'compound': compound}}, {'$limit': limit},
            {'$project': {'filing_id': 1, 'filing_type': 1, 'matched': 1, 'duns_numbers': 1, 'role_players': 1, 'score': {'$meta': 'searchScore'}}}]

def relaxed_search(db, collection, name, street='', city='', state='', limit=10):
    coll = db[collection]
    attempts = [{'street': street, 'city': city, 'state': state, 'level': 'Full address'},
                {'street': '', 'city': city, 'state': state, 'level': 'City + State'},
                {'street': '', 'city': '', 'state': state, 'level': 'State only'},
                {'street': '', 'city': '', 'state': '', 'level': 'Name only (national)'}]
    if not street: attempts = attempts[1:]
    results_by_level = []
    for attempt in attempts:
        pipeline = build_core_search(name, attempt['city'], attempt['state'], attempt['street'], limit)
        try: results = list(coll.aggregate(pipeline))
        except Exception: results = []
        results_by_level.append({'level': attempt['level'], 'count': len(results), 'results': results})
        if results: break
    return results_by_level

TYPO_OPS = [('swap', 'Swapped two adjacent letters'), ('drop', 'Dropped a letter'), ('add', 'Added a random letter'), ('replace', 'Replaced a letter')]
def introduce_typo(text):
    if len(text) < 3: return text, 'Too short'
    chars = list(text)
    op, desc = random.choice(TYPO_OPS)
    idx = random.randint(1, len(chars) - 2)
    if op == 'swap' and idx < len(chars) - 1: chars[idx], chars[idx+1] = chars[idx+1], chars[idx]
    elif op == 'drop': chars.pop(idx)
    elif op == 'add': chars.insert(idx, random.choice('abcdefghijklmnopqrstuvwxyz'))
    elif op == 'replace': chars[idx] = random.choice('abcdefghijklmnopqrstuvwxyz')
    return ''.join(chars), desc


# ================================================================== #
#  API ROUTES                                                          #
# ================================================================== #

# --- Change Stream ---
@app.route('/api/cs/stream')
def cs_stream():
    def generate():
        with cs_stats_lock:
            snap = dict(cs_stats)
        snap['type'] = 'stats'
        yield f"data: {json.dumps(snap)}\n\n"

        with recent_events_lock:
            buffered = list(recent_events)
        for ev in buffered:
            yield f"data: {json.dumps(ev)}\n\n"

        while True:
            try:
                data = event_queue.get(timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

# --- Search ---
@app.route('/api/search', methods=['POST'])
def api_search():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    body = request.json or {}
    name = (body.get('name') or '').strip()
    city = (body.get('city') or '').strip()
    state = (body.get('state') or '').strip()
    street = (body.get('street') or '').strip()
    search_type = body.get('type', 'core')
    limit = min(int(body.get('limit', 10)), 25)
    if not name and not city and not state and not street:
        return jsonify({'error': 'Enter at least one search term (name, city, state, or street)'}), 400
    tic = time.time()
    try:
        pipeline = None
        if search_type == 'core':
            pipeline = build_core_search(name, city, state, street, limit)
            results = list(db['duns'].aggregate(pipeline))
        elif search_type == 'trade':
            pipeline = build_trade_search(name, city, state, limit)
            results = list(db['entity_trade'].aggregate(pipeline))
        elif search_type == 'public_records':
            results = []
            for cn in PR_COLLECTIONS:
                try:
                    pipeline = build_public_records_search(name, city, state, limit)
                    r = list(db[cn].aggregate(pipeline))
                    for d in r: d['_source_collection'] = cn
                    results.extend(r)
                except Exception: pass
            results.sort(key=lambda x: x.get('score', 0), reverse=True)
            results = results[:limit]
        else: return jsonify({'error': f'Unknown type: {search_type}'}), 400
        elapsed = (time.time() - tic) * 1000
        for r in results:
            if '_id' in r: r['_id'] = str(r['_id'])
        resp = {'results': results, 'count': len(results), 'elapsed_ms': round(elapsed, 1), 'query': {'name': name, 'city': city, 'state': state, 'street': street}, 'type': search_type}
        if pipeline:
            resp['pipeline'] = json.loads(json.dumps(pipeline, default=str))
        return jsonify(resp)
    except pymongo.errors.OperationFailure as e:
        elapsed = (time.time() - tic) * 1000
        err = str(e)
        if 'index not found' in err.lower(): return jsonify({'error': 'Atlas Search index not found.', 'detail': err, 'elapsed_ms': round(elapsed, 1)}), 400
        return jsonify({'error': err, 'elapsed_ms': round(elapsed, 1)}), 500
    except Exception as e:
        return jsonify({'error': str(e), 'elapsed_ms': round((time.time()-tic)*1000, 1)}), 500

@app.route('/api/relaxation', methods=['POST'])
def api_relaxation():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    body = request.json or {}
    name = (body.get('name') or '').strip()
    if not name: return jsonify({'error': 'Name is required'}), 400
    tic = time.time()
    try:
        levels = relaxed_search(db, 'duns', name, body.get('street','').strip(), body.get('city','').strip(), body.get('state','').strip(), min(int(body.get('limit',10)),25))
        for lv in levels:
            for r in lv['results']:
                if '_id' in r: r['_id'] = str(r['_id'])
        return jsonify({'levels': levels, 'elapsed_ms': round((time.time()-tic)*1000, 1)})
    except Exception as e:
        return jsonify({'error': str(e), 'elapsed_ms': round((time.time()-tic)*1000, 1)}), 500

@app.route('/api/typo-demo', methods=['POST'])
def api_typo_demo():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    body = request.json or {}
    name = (body.get('name') or '').strip()
    limit = min(int(body.get('limit', 10)), 25)
    if not name: return jsonify({'error': 'Name is required'}), 400
    typo_name, typo_desc = introduce_typo(name)
    tic = time.time()
    try:
        orig = list(db['duns'].aggregate(build_core_search(name, limit=limit)))
        typo = list(db['duns'].aggregate(build_core_search(typo_name, limit=limit)))
        elapsed = (time.time() - tic) * 1000
        for r in orig + typo:
            if '_id' in r: r['_id'] = str(r['_id'])
        od = {r.get('dunsNumber') for r in orig}; td = {r.get('dunsNumber') for r in typo}; overlap = od & td
        return jsonify({'original': {'name': name, 'results': orig, 'count': len(orig)}, 'typo': {'name': typo_name, 'description': typo_desc, 'results': typo, 'count': len(typo)}, 'overlap_count': len(overlap), 'overlap_pct': round(len(overlap)/max(len(od),1)*100, 1), 'elapsed_ms': round(elapsed, 1)})
    except Exception as e:
        return jsonify({'error': str(e), 'elapsed_ms': round((time.time()-tic)*1000, 1)}), 500

@app.route('/api/sample-names')
def api_sample_names():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    try:
        results = list(db['duns'].aggregate([{'$sample': {'size': 8}}, {'$project': {'current.name': 1, 'current.address.city': 1, 'current.address.state': 1, 'dunsNumber': 1, '_id': 0}}]))
        return jsonify({'samples': results})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/document/<duns_number>')
def api_document(duns_number):
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    tic = time.time()
    try:
        doc = db['duns'].find_one({'dunsNumber': duns_number})
        elapsed = (time.time()-tic)*1000
        if not doc: return jsonify({'error': 'Not found'}), 404
        doc['_id'] = str(doc['_id'])
        for key in ('meta', 'financials', 'assessment'):
            if key in doc: doc[key] = json.loads(json.dumps(doc[key], default=str))
        return jsonify({'document': doc, 'elapsed_ms': round(elapsed, 1)})
    except Exception as e: return jsonify({'error': str(e)}), 500

# --- Graph ---
@app.route('/api/graph/duns/<duns_number>')
def api_graph_by_duns(duns_number):
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    tic = time.time()
    nodes = {}; edges = []; filing_ids_seen = set()
    core_doc = db['duns'].find_one({'dunsNumber': duns_number}, {'dunsNumber': 1, 'current.name': 1, 'current.address': 1, 'assessment.credit_score': 1, 'legal.status': 1})
    origin_name = core_doc['current']['name'] if core_doc and 'current' in core_doc else f'DUNS {duns_number}'
    origin_addr = ''
    if core_doc and 'current' in core_doc:
        a = core_doc['current'].get('address', {})
        origin_addr = ', '.join(filter(None, [a.get('city'), a.get('state')]))
    nodes[f'duns-{duns_number}'] = {'id': f'duns-{duns_number}', 'type': 'duns', 'label': origin_name, 'duns': duns_number, 'address': origin_addr, 'credit_score': core_doc.get('assessment', {}).get('credit_score') if core_doc else None, 'status': core_doc.get('legal', {}).get('status') if core_doc else None, 'matched': True, 'is_origin': True}

    for coll_name in PR_COLLECTIONS:
        try:
            cnt = db[coll_name].estimated_document_count()
            if cnt == 0: continue
        except Exception: continue
        filings = list(db[coll_name].find({'duns_numbers': duns_number}, {'filing_id': 1, 'filing_type': 1, 'filed_date': 1, 'filing_status': 1, 'court': 1, 'amount': 1, 'jurisdiction': 1, 'role_players': 1}).limit(20))
        for filing in filings:
            fid = filing.get('filing_id', str(filing.get('_id', '')))
            if fid in filing_ids_seen: continue
            filing_ids_seen.add(fid)
            fn = f'filing-{fid}'
            ftype = filing.get('filing_type', coll_name)
            nodes[fn] = {'id': fn, 'type': 'filing', 'filing_type': ftype, 'label': f'{ftype.upper()} {fid[:10]}', 'filing_id': fid, 'filed_date': filing.get('filed_date', ''), 'filing_status': filing.get('filing_status', ''), 'court': filing.get('court', ''), 'jurisdiction': filing.get('jurisdiction', ''), 'amount': filing.get('amount')}
            for rp in filing.get('role_players', []):
                rp_duns = rp.get('duns_number'); rp_role = rp.get('role_type', '?'); rp_polarity = rp.get('polarity', '?')
                rp_names = rp.get('names', []); rp_name = rp_names[0]['name'] if rp_names else '?'
                rp_addrs = rp.get('addresses', [])
                rp_addr = ', '.join(filter(None, [rp_addrs[0].get('city', ''), rp_addrs[0].get('state', '')])) if rp_addrs else ''
                if rp_duns:
                    rn = f'duns-{rp_duns}'
                    if rn not in nodes:
                        nodes[rn] = {'id': rn, 'type': 'duns', 'label': rp_name, 'duns': rp_duns, 'address': rp_addr, 'matched': True, 'is_origin': False}
                else:
                    rn = f'unmatched-{fn}-{rp_role}'
                    nodes[rn] = {'id': rn, 'type': 'unmatched', 'label': rp_name, 'address': rp_addr, 'matched': False, 'role_type': rp_role}
                edges.append({'source': rn, 'target': fn, 'role': rp_role, 'polarity': rp_polarity})
    elapsed = (time.time()-tic)*1000
    return jsonify({'nodes': list(nodes.values()), 'edges': edges, 'origin_duns': duns_number, 'filing_count': len(filing_ids_seen), 'entity_count': sum(1 for n in nodes.values() if n['type']=='duns'), 'unmatched_count': sum(1 for n in nodes.values() if n['type']=='unmatched'), 'elapsed_ms': round(elapsed, 1)})

@app.route('/api/sample-duns')
def api_sample_duns():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    samples = []
    for coll_name in PR_COLLECTIONS:
        try:
            if db[coll_name].estimated_document_count() == 0: continue
            docs = list(db[coll_name].aggregate([{'$sample': {'size': 30}}, {'$match': {'duns_numbers': {'$ne': []}}}, {'$limit': 4}, {'$project': {'duns_numbers': 1, 'filing_type': 1}}], maxTimeMS=10000))
            for doc in docs:
                for d in doc.get('duns_numbers', [])[:1]:
                    core = db['duns'].find_one({'dunsNumber': d}, {'current.name': 1, '_id': 0})
                    samples.append({'duns': d, 'name': core['current']['name'] if core and 'current' in core else f'DUNS {d}', 'found_in': doc.get('filing_type', coll_name)})
        except Exception: continue
    return jsonify({'samples': samples[:12]})

@app.route('/api/stats')
def api_stats():
    db = get_db()
    if db is None: return jsonify({'error': 'Database not connected'}), 500
    try:
        s = {'core': db['duns'].estimated_document_count(), 'trade': db['entity_trade'].estimated_document_count(), 'audit': db['duns_audit'].estimated_document_count()}
        s['public_records'] = sum(db[c].estimated_document_count() for c in PR_COLLECTIONS)
        return jsonify(s)
    except Exception as e: return jsonify({'error': str(e)}), 500


# ================================================================== #
#  LOCUST PROCESS MANAGER                                              #
# ================================================================== #
import subprocess, signal, socket, atexit

LOCUST_SCRIPTS = {
    'seed-core': {'file': 'locust_01_seed_core.py', 'port': 8089, 'label': 'Seed Core',
                  'env_extra': {'MONGODB_COLLECTION': 'duns', 'LOCUST_BULK_SIZE': '100'}},
    'seed-pr':   {'file': 'locust_02_seed_public_records.py', 'port': 8090, 'label': 'Seed Public Records',
                  'env_extra': {'MONGODB_COLLECTION': 'suits', 'LOCUST_BULK_SIZE': '200'}},
    'search':    {'file': 'locust_03_search.py', 'port': 8091, 'label': 'Search Benchmark',
                  'env_extra': {'LOCUST_CANDIDATE_LIMIT': '10'}},
    'ops':       {'file': 'locust_04_ops.py', 'port': 8092, 'label': 'Ops & Change Detection',
                  'env_extra': {'MONGODB_COLLECTION': 'duns', 'LOCUST_BULK_FILE_SIZE': '50'}},
}
_locust_procs = {}


def _port_open(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex(('127.0.0.1', port)) == 0
    except Exception:
        return False


def _start_locust(key):
    spec = LOCUST_SCRIPTS[key]
    if _port_open(spec['port']):
        return {'status': 'already_running', 'port': spec['port']}

    if key in _locust_procs and _locust_procs[key].poll() is None:
        return {'status': 'already_running', 'port': spec['port']}

    env = dict(os.environ)
    cfg = resolve_config()
    if cfg:
        env['MONGODB_URI'] = cfg.connection_string
        env['MONGODB_DATABASE'] = cfg.database_name
    env.update(spec.get('env_extra', {}))

    cwd = os.path.dirname(os.path.abspath(__file__))
    cmd = [sys.executable, '-m', 'locust', '-f', spec['file'], '--web-port', str(spec['port'])]
    proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _locust_procs[key] = proc
    return {'status': 'started', 'port': spec['port'], 'pid': proc.pid}


def _stop_locust(key):
    proc = _locust_procs.get(key)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        del _locust_procs[key]
        return {'status': 'stopped'}
    return {'status': 'not_running'}


def _cleanup_locust():
    for key in list(_locust_procs.keys()):
        _stop_locust(key)

atexit.register(_cleanup_locust)


@app.route('/api/locust/start/<key>')
def api_locust_start(key):
    if key not in LOCUST_SCRIPTS:
        return jsonify({'error': f'Unknown script: {key}'}), 400
    return jsonify(_start_locust(key))


@app.route('/api/locust/stop/<key>')
def api_locust_stop(key):
    if key not in LOCUST_SCRIPTS:
        return jsonify({'error': f'Unknown script: {key}'}), 400
    return jsonify(_stop_locust(key))


@app.route('/api/locust/status')
def api_locust_status():
    status = {}
    for key, spec in LOCUST_SCRIPTS.items():
        running = _port_open(spec['port'])
        status[key] = {'port': spec['port'], 'running': running, 'label': spec['label']}
    return jsonify(status)


# ================================================================== #
#  PAGE ROUTES — each serves full HTML with shared nav                 #
# ================================================================== #

def _nav_link(href, label, active, key):
    is_active = active == key
    color = '#58a6ff' if is_active else '#8b949e'
    weight = '600' if is_active else '400'
    border = '#58a6ff' if is_active else 'transparent'
    return f'<a href="{href}" style="padding:10px 12px;color:{color};text-decoration:none;font-size:13px;font-weight:{weight};border-bottom:2px solid {border};transition:all 0.15s;white-space:nowrap;">{label}</a>'


def _nav(active):
    sep = '<span style="width:1px;height:20px;background:#30363d;margin:0 2px;"></span>'
    return f'''<nav style="display:flex;align-items:center;gap:0;background:#0d1117;border-bottom:1px solid #30363d;padding:0 12px;position:sticky;top:0;z-index:100;overflow-x:auto;">
  <a href="/" style="padding:10px 14px;color:#e6edf3;text-decoration:none;font-weight:700;font-size:14px;border-right:1px solid #30363d;margin-right:4px;white-space:nowrap;">PoC Hub</a>
  {_nav_link('/change-stream','Change Stream',active,'cs')}
  {_nav_link('/search','Search Explorer',active,'se')}
  {_nav_link('/graph','Relationship Graph',active,'gr')}
  {sep}
  {_nav_link('/locust/seed-core','Seed Core',active,'l-seed-core')}
  {_nav_link('/locust/seed-pr','Seed Public Records',active,'l-seed-pr')}
  {_nav_link('/locust/search','Search Benchmark',active,'l-search')}
  {_nav_link('/locust/ops','Ops &amp; Changes',active,'l-ops')}
  <span style="margin-left:auto;font-size:10px;color:#484f58;white-space:nowrap;">MongoDB Solutions Architecture</span>
</nav>'''


@app.route('/')
def hub_index():
    html = f'''<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>MongoDB Atlas PoC — Demo Hub</title>
<style>*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0d1117;color:#c9d1d9;}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:20px;padding:20px 60px;max-width:1400px;margin:0 auto;}}
.card{{padding:32px;border-radius:12px;border:1px solid #30363d;background:#161b22;transition:all 0.2s;cursor:pointer;text-decoration:none;color:inherit;display:block;}}
.card:hover{{border-color:#58a6ff;transform:translateY(-2px);box-shadow:0 4px 20px rgba(88,166,255,0.1);}}
.card h2{{font-size:18px;color:#e6edf3;margin-bottom:8px;}}
.card .ask{{font-size:12px;color:#58a6ff;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;}}
.card p{{font-size:13px;color:#8b949e;line-height:1.6;}}
.hero{{text-align:center;padding:48px 32px 24px;}}
.hero h1{{font-size:28px;color:#e6edf3;margin-bottom:8px;}}
.hero p{{font-size:14px;color:#8b949e;}}
.stats-row{{display:flex;justify-content:center;gap:32px;padding:16px;}}
.sr{{text-align:center;}} .sr .sv{{font-size:22px;font-weight:700;color:#58a6ff;font-variant-numeric:tabular-nums;}} .sr .sl{{font-size:11px;color:#484f58;text-transform:uppercase;}}
</style></head><body>
{_nav('home')}
<div class="hero"><h1>MongoDB Atlas PoC Demo Hub</h1><p>MongoDB Atlas — Fuzzy Search, Change Detection, Many-to-Many Relationships, Load Testing</p></div>
<div class="stats-row"><div class="sr"><span class="sv" id="h-core">—</span><br><span class="sl">Core Entities</span></div><div class="sr"><span class="sv" id="h-pr">—</span><br><span class="sl">Public Records</span></div><div class="sr"><span class="sv" id="h-trade">—</span><br><span class="sl">Trade Records</span></div><div class="sr"><span class="sv" id="h-audit">—</span><br><span class="sl">Audit Events</span></div></div>
<div class="cards">
  <a class="card" href="/search"><div class="ask">Fuzzy Search</div><h2>Atlas Search Explorer</h2><p>Fuzzy name + address search across core entities, trade accounts, and public records. Typo resilience demo and address relaxation.</p></a>
  <a class="card" href="/change-stream"><div class="ask">Change Detection</div><h2>Change Stream Dashboard</h2><p>Real-time change detection feed. See publish vs. suppress events as bulk files are processed. Only changed records go downstream.</p></a>
  <a class="card" href="/graph"><div class="ask">Many-to-Many</div><h2>Relationship Graph</h2><p>Interactive force-directed graph showing many-to-many relationships between DUNS entities and public record filings via role players.</p></a>
</div>
<h2 style="text-align:center;font-size:16px;color:#8b949e;margin:24px 0 16px;text-transform:uppercase;letter-spacing:2px;">Load Testing</h2>
<div class="cards">
  <a class="card" href="/locust/seed-core"><div class="ask">Seed</div><h2>Seed Core Entities</h2><p>Bulk upsert synthetic DUNS records into the core collection. Simulates initial data load of the entity universe.</p></a>
  <a class="card" href="/locust/seed-pr"><div class="ask">Seed</div><h2>Seed Public Records</h2><p>Insert synthetic suits, liens, judgments, UCCs, and bankruptcies with role players and many-to-many linkages.</p></a>
  <a class="card" href="/locust/search"><div class="ask">Benchmark</div><h2>Search Benchmark</h2><p>Run concurrent fuzzy name + address searches across core, trade, and public records. Measures throughput and latency.</p></a>
  <a class="card" href="/locust/ops"><div class="ask">Benchmark</div><h2>Ops &amp; Change Detection</h2><p>Simulate agent updates, financial refreshes, score updates, and bulk file processing with publish/suppress detection.</p></a>
</div>
<script>fetch('/api/stats').then(r=>r.json()).then(d=>{{if(!d.error){{document.getElementById('h-core').textContent=(d.core||0).toLocaleString();document.getElementById('h-pr').textContent=(d.public_records||0).toLocaleString();document.getElementById('h-trade').textContent=(d.trade||0).toLocaleString();document.getElementById('h-audit').textContent=(d.audit||0).toLocaleString();}}}});</script>
</body></html>'''
    return html


@app.route('/change-stream')
def page_change_stream():
    from change_stream_dashboard import DASHBOARD_HTML
    patched = DASHBOARD_HTML.replace('</body>', '')
    patched = patched.replace('<body>', f'<body>{_nav("cs")}')
    patched = patched.replace("'/stream'", "'/api/cs/stream'")
    patched += '</body></html>'
    return patched


@app.route('/search')
def page_search():
    from search_explorer import EXPLORER_HTML
    patched = EXPLORER_HTML.replace('<body>', f'<body>{_nav("se")}')
    patched = patched.replace("calc(100vh - 60px)", "calc(100vh - 100px)")
    return patched


@app.route('/graph')
def page_graph():
    from relationship_graph import GRAPH_HTML
    patched = GRAPH_HTML.replace('<body>', f'<body>{_nav("gr")}')
    patched = patched.replace("calc(100vh - 90px)", "calc(100vh - 130px)")
    return patched


@app.route('/locust/<key>')
def page_locust(key):
    if key not in LOCUST_SCRIPTS:
        return 'Unknown script', 404
    spec = LOCUST_SCRIPTS[key]
    port = spec['port']
    label = spec['label']
    nav_key = f'l-{key}'
    return f'''<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>MongoDB Atlas PoC — {label}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0d1117;color:#c9d1d9;}}
.locust-bar{{display:flex;align-items:center;gap:12px;padding:8px 20px;background:#161b22;border-bottom:1px solid #30363d;}}
.locust-bar .status{{width:10px;height:10px;border-radius:50%;}}
.locust-bar .status.on{{background:#3fb950;box-shadow:0 0 6px #3fb95088;}}
.locust-bar .status.off{{background:#f85149;}}
.locust-bar .label{{font-size:13px;color:#e6edf3;font-weight:600;}}
.locust-bar button{{padding:5px 16px;border-radius:6px;border:1px solid #30363d;font-size:12px;cursor:pointer;transition:all 0.15s;font-weight:500;}}
.btn-start{{background:#238636;color:#fff;border-color:#238636;}}
.btn-start:hover{{background:#2ea043;}}
.btn-stop{{background:#da3633;color:#fff;border-color:#da3633;}}
.btn-stop:hover{{background:#f85149;}}
.locust-bar .port{{font-size:11px;color:#484f58;margin-left:auto;}}
iframe{{width:100%;height:calc(100vh - 85px);border:none;background:#fff;}}
.loading{{display:flex;align-items:center;justify-content:center;height:calc(100vh - 85px);flex-direction:column;gap:12px;}}
.loading .spinner{{width:32px;height:32px;border:3px solid #30363d;border-top-color:#58a6ff;border-radius:50%;animation:spin 0.8s linear infinite;}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
</style></head><body>
{_nav(nav_key)}
<div class="locust-bar">
  <div class="status off" id="indicator"></div>
  <span class="label">{label}</span>
  <button class="btn-start" id="btnStart" onclick="startLocust()">Start</button>
  <button class="btn-stop" id="btnStop" onclick="stopLocust()" style="display:none;">Stop</button>
  <span class="port" id="portLabel">Port {port}</span>
</div>
<div id="frame-area">
  <div class="loading" id="loader"><div class="spinner"></div><span style="color:#8b949e;font-size:13px;">Click <b>Start</b> to launch Locust on port {port}</span></div>
</div>
<script>
const KEY="{key}", PORT={port};
let polling=null;

function startLocust() {{
  document.getElementById('btnStart').disabled=true;
  document.getElementById('btnStart').textContent='Starting…';
  fetch('/api/locust/start/'+KEY).then(r=>r.json()).then(d=>{{
    pollReady();
  }});
}}

function stopLocust() {{
  fetch('/api/locust/stop/'+KEY).then(r=>r.json()).then(d=>{{
    document.getElementById('indicator').className='status off';
    document.getElementById('btnStart').style.display='';
    document.getElementById('btnStart').disabled=false;
    document.getElementById('btnStart').textContent='Start';
    document.getElementById('btnStop').style.display='none';
    document.getElementById('frame-area').innerHTML='<div class="loading" id="loader"><div class="spinner" style="animation:none;border-color:#30363d;"></div><span style="color:#8b949e;font-size:13px;">Locust stopped. Click <b>Start</b> to relaunch.</span></div>';
    if(polling) clearInterval(polling);
  }});
}}

function pollReady() {{
  let attempts=0;
  polling=setInterval(()=>{{
    attempts++;
    fetch('http://localhost:'+PORT, {{mode:'no-cors'}}).then(()=>{{
      clearInterval(polling);
      showFrame();
    }}).catch(()=>{{
      if(attempts>40) {{
        clearInterval(polling);
        document.getElementById('btnStart').textContent='Retry';
        document.getElementById('btnStart').disabled=false;
      }}
    }});
  }}, 500);
}}

function showFrame() {{
  document.getElementById('indicator').className='status on';
  document.getElementById('btnStart').style.display='none';
  document.getElementById('btnStop').style.display='';
  document.getElementById('frame-area').innerHTML='<iframe src="http://localhost:'+PORT+'" id="locust-frame"></iframe>';
}}

fetch('/api/locust/status').then(r=>r.json()).then(data=>{{
  if(data[KEY] && data[KEY].running) showFrame();
}});
</script></body></html>'''


# ================================================================== #
#  MAIN                                                                #
# ================================================================== #

def main():
    parser = argparse.ArgumentParser(description='MongoDB Atlas PoC — Unified Demo Hub')
    parser.add_argument('--port', type=int, default=5050, help='Web server port (default: 5050)')
    parser.add_argument('--uri', help='MongoDB connection string (or set MONGODB_URI)')
    parser.add_argument('--db', help='Database name (or set MONGODB_DATABASE)')
    parser.add_argument('--collection', default=None, help='Collection to watch for change stream (default: duns)')
    args = parser.parse_args()

    cfg = resolve_config()
    uri = args.uri or (cfg.connection_string if cfg else None)
    db_name = args.db or (cfg.database_name if cfg else None)
    collection = args.collection or (cfg.collection_name if cfg else 'duns')

    if not uri or not db_name:
        print('Set MONGODB_URI and MONGODB_DATABASE env vars, or pass --uri and --db.')
        sys.exit(1)

    t = threading.Thread(target=watch_changes, args=(uri, db_name, collection), daemon=True)
    t.start()

    print(f'[demo-hub] Database: {db_name}')
    print(f'[demo-hub] Change stream watching: {collection}')
    print(f'[demo-hub] Open http://localhost:{args.port} in your browser')
    app.run(host='0.0.0.0', port=args.port, threaded=True)


if __name__ == '__main__':
    main()
