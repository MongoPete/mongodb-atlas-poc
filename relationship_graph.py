#!/usr/bin/env python

'''
MongoDB Atlas PoC — Many-to-Many Relationship Graph Explorer
============================================================
Interactive D3.js force-directed graph showing the many-to-many
relationships between DUNS entities and public record filings
(suits, liens, judgments, UCCs, bankruptcies) via role players.

Demonstrates:
  "Show me the data model handles the many-to-many problem."

Usage:
    export MONGODB_URI="mongodb+srv://..."
    export MONGODB_DATABASE="poc_demo"
    python relationship_graph.py

    Then open http://localhost:5080 in your browser.
'''

import os
import sys
import json
import time
import argparse

import pymongo
from flask import Flask, request, jsonify, render_template_string

from locust_db_config import resolve_config

app = Flask(__name__)

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


def _esc(s):
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


# ------------------------------------------------------------------ #
#  API: Build the relationship graph for a DUNS number                 #
# ------------------------------------------------------------------ #

@app.route('/api/graph/duns/<duns_number>')
def api_graph_by_duns(duns_number):
    '''Given a DUNS number, find all filings it appears in across all PR collections,
    then find all other DUNS entities in those filings — building the many-to-many graph.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    tic = time.time()
    nodes = {}
    edges = []
    filing_ids_seen = set()

    origin_node_id = f'duns-{duns_number}'
    core_doc = db['duns'].find_one(
        {'dunsNumber': duns_number},
        {'dunsNumber': 1, 'current.name': 1, 'current.address': 1,
         'assessment.credit_score': 1, 'legal.status': 1}
    )
    origin_name = core_doc['current']['name'] if core_doc and 'current' in core_doc else f'DUNS {duns_number}'
    origin_addr = ''
    if core_doc and 'current' in core_doc:
        a = core_doc['current'].get('address', {})
        origin_addr = ', '.join(filter(None, [a.get('city'), a.get('state')]))

    nodes[origin_node_id] = {
        'id': origin_node_id,
        'type': 'duns',
        'label': origin_name,
        'duns': duns_number,
        'address': origin_addr,
        'credit_score': core_doc.get('assessment', {}).get('credit_score') if core_doc else None,
        'status': core_doc.get('legal', {}).get('status') if core_doc else None,
        'matched': True,
        'is_origin': True,
    }

    for coll_name in PR_COLLECTIONS:
        coll = db[coll_name]
        filings = list(coll.find(
            {'duns_numbers': duns_number},
            {'filing_id': 1, 'filing_type': 1, 'filed_date': 1, 'filing_status': 1,
             'court': 1, 'amount': 1, 'jurisdiction': 1, 'matched': 1,
             'role_players': 1, 'duns_numbers': 1}
        ).limit(20))

        for filing in filings:
            fid = filing.get('filing_id', str(filing.get('_id', '')))
            if fid in filing_ids_seen:
                continue
            filing_ids_seen.add(fid)

            filing_node_id = f'filing-{fid}'
            ftype = filing.get('filing_type', coll_name)
            amount = filing.get('amount')

            nodes[filing_node_id] = {
                'id': filing_node_id,
                'type': 'filing',
                'filing_type': ftype,
                'label': f'{ftype.upper()} {fid[:10]}',
                'filing_id': fid,
                'filed_date': filing.get('filed_date', ''),
                'filing_status': filing.get('filing_status', ''),
                'court': filing.get('court', ''),
                'jurisdiction': filing.get('jurisdiction', ''),
                'amount': amount,
            }

            for rp in filing.get('role_players', []):
                rp_duns = rp.get('duns_number')
                rp_role = rp.get('role_type', '?')
                rp_polarity = rp.get('polarity', '?')
                rp_matched = rp.get('matched', False)
                rp_names = rp.get('names', [])
                rp_name = rp_names[0]['name'] if rp_names else '?'
                rp_addrs = rp.get('addresses', [])
                rp_addr = ', '.join(filter(None, [
                    rp_addrs[0].get('city', ''), rp_addrs[0].get('state', '')
                ])) if rp_addrs else ''

                if rp_duns:
                    rp_node_id = f'duns-{rp_duns}'
                    if rp_node_id not in nodes:
                        rp_core = db['duns'].find_one(
                            {'dunsNumber': rp_duns},
                            {'current.name': 1, 'current.address': 1}
                        )
                        if rp_core and 'current' in rp_core:
                            rp_name = rp_core['current'].get('name', rp_name)
                            ra = rp_core['current'].get('address', {})
                            rp_addr = ', '.join(filter(None, [ra.get('city'), ra.get('state')]))

                        nodes[rp_node_id] = {
                            'id': rp_node_id,
                            'type': 'duns',
                            'label': rp_name,
                            'duns': rp_duns,
                            'address': rp_addr,
                            'matched': True,
                            'is_origin': False,
                        }
                else:
                    rp_node_id = f'unmatched-{filing_node_id}-{rp_role}'
                    nodes[rp_node_id] = {
                        'id': rp_node_id,
                        'type': 'unmatched',
                        'label': rp_name,
                        'address': rp_addr,
                        'matched': False,
                        'role_type': rp_role,
                    }

                edges.append({
                    'source': rp_node_id,
                    'target': filing_node_id,
                    'role': rp_role,
                    'polarity': rp_polarity,
                })

    elapsed = (time.time() - tic) * 1000
    return jsonify({
        'nodes': list(nodes.values()),
        'edges': edges,
        'origin_duns': duns_number,
        'filing_count': len(filing_ids_seen),
        'entity_count': sum(1 for n in nodes.values() if n['type'] == 'duns'),
        'unmatched_count': sum(1 for n in nodes.values() if n['type'] == 'unmatched'),
        'elapsed_ms': round(elapsed, 1),
    })


@app.route('/api/graph/filing/<filing_id>')
def api_graph_by_filing(filing_id):
    '''Given a filing ID, show all role players and their DUNS connections.'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    tic = time.time()
    nodes = {}
    edges = []

    filing = None
    source_coll = None
    for coll_name in PR_COLLECTIONS:
        filing = db[coll_name].find_one({'filing_id': filing_id})
        if filing:
            source_coll = coll_name
            break

    if not filing:
        return jsonify({'error': 'Filing not found'}), 404

    ftype = filing.get('filing_type', source_coll)
    filing_node_id = f'filing-{filing_id}'
    nodes[filing_node_id] = {
        'id': filing_node_id,
        'type': 'filing',
        'filing_type': ftype,
        'label': f'{ftype.upper()} {filing_id[:10]}',
        'filing_id': filing_id,
        'filed_date': filing.get('filed_date', ''),
        'filing_status': filing.get('filing_status', ''),
        'court': filing.get('court', ''),
        'jurisdiction': filing.get('jurisdiction', ''),
        'amount': filing.get('amount'),
    }

    for rp in filing.get('role_players', []):
        rp_duns = rp.get('duns_number')
        rp_role = rp.get('role_type', '?')
        rp_polarity = rp.get('polarity', '?')
        rp_names = rp.get('names', [])
        rp_name = rp_names[0]['name'] if rp_names else '?'
        rp_addrs = rp.get('addresses', [])
        rp_addr = ', '.join(filter(None, [
            rp_addrs[0].get('city', ''), rp_addrs[0].get('state', '')
        ])) if rp_addrs else ''

        if rp_duns:
            rp_node_id = f'duns-{rp_duns}'
            if rp_node_id not in nodes:
                rp_core = db['duns'].find_one(
                    {'dunsNumber': rp_duns},
                    {'current.name': 1, 'current.address': 1}
                )
                if rp_core and 'current' in rp_core:
                    rp_name = rp_core['current'].get('name', rp_name)
                    ra = rp_core['current'].get('address', {})
                    rp_addr = ', '.join(filter(None, [ra.get('city'), ra.get('state')]))

                nodes[rp_node_id] = {
                    'id': rp_node_id,
                    'type': 'duns',
                    'label': rp_name,
                    'duns': rp_duns,
                    'address': rp_addr,
                    'matched': True,
                    'is_origin': False,
                }

            # Also find other filings this DUNS is in
            for coll_name in PR_COLLECTIONS:
                other_filings = list(db[coll_name].find(
                    {'duns_numbers': rp_duns, 'filing_id': {'$ne': filing_id}},
                    {'filing_id': 1, 'filing_type': 1, 'filed_date': 1, 'filing_status': 1}
                ).limit(5))
                for of in other_filings:
                    of_id = of.get('filing_id', '')
                    of_node_id = f'filing-{of_id}'
                    if of_node_id not in nodes:
                        nodes[of_node_id] = {
                            'id': of_node_id,
                            'type': 'filing',
                            'filing_type': of.get('filing_type', coll_name),
                            'label': f'{of.get("filing_type", coll_name).upper()} {of_id[:10]}',
                            'filing_id': of_id,
                            'filed_date': of.get('filed_date', ''),
                            'filing_status': of.get('filing_status', ''),
                        }
                    edges.append({
                        'source': rp_node_id,
                        'target': of_node_id,
                        'role': rp_role,
                        'polarity': rp_polarity,
                    })
        else:
            rp_node_id = f'unmatched-{filing_node_id}-{rp_role}'
            nodes[rp_node_id] = {
                'id': rp_node_id,
                'type': 'unmatched',
                'label': rp_name,
                'address': rp_addr,
                'matched': False,
                'role_type': rp_role,
            }

        edges.append({
            'source': rp_node_id,
            'target': filing_node_id,
            'role': rp_role,
            'polarity': rp_polarity,
        })

    elapsed = (time.time() - tic) * 1000
    return jsonify({
        'nodes': list(nodes.values()),
        'edges': edges,
        'elapsed_ms': round(elapsed, 1),
    })


@app.route('/api/sample-duns')
def api_sample_duns():
    '''Get sample DUNS numbers that appear in public records (for quick demo).'''
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500

    samples = []
    for coll_name in PR_COLLECTIONS:
        try:
            count = db[coll_name].estimated_document_count()
            if count == 0:
                continue
            docs = list(db[coll_name].aggregate([
                {'$sample': {'size': 30}},
                {'$match': {'duns_numbers': {'$ne': []}}},
                {'$limit': 4},
                {'$project': {'duns_numbers': 1, 'filing_type': 1, 'filing_id': 1}},
            ], maxTimeMS=10000))
            for doc in docs:
                for d in doc.get('duns_numbers', [])[:1]:
                    core = db['duns'].find_one({'dunsNumber': d}, {'current.name': 1, '_id': 0})
                    name = core['current']['name'] if core and 'current' in core else f'DUNS {d}'
                    samples.append({
                        'duns': d,
                        'name': name,
                        'found_in': doc.get('filing_type', coll_name),
                    })
        except Exception:
            continue
    return jsonify({'samples': samples[:12]})


@app.route('/api/collection-stats')
def api_collection_stats():
    db = get_db()
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500
    stats = {}
    stats['core'] = db['duns'].estimated_document_count()
    for c in PR_COLLECTIONS:
        stats[c] = db[c].estimated_document_count()
    return jsonify(stats)


# ------------------------------------------------------------------ #
#  UI                                                                  #
# ------------------------------------------------------------------ #

GRAPH_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MongoDB Atlas PoC — Many-to-Many Relationship Graph</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --border: #30363d;
  --text: #c9d1d9; --text2: #8b949e; --text3: #484f58;
  --blue: #58a6ff; --green: #3fb950; --yellow: #d29922;
  --purple: #d2a8ff; --orange: #f0883e; --red: #f85149;
  --cyan: #56d4dd; --pink: #f778ba;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); overflow: hidden; }

.header {
  background: var(--bg2); border-bottom: 1px solid var(--border);
  padding: 12px 24px; display: flex; align-items: center; gap: 16px; z-index: 10; position: relative;
}
.header h1 { font-size: 18px; font-weight: 600; color: #e6edf3; }
.header .subtitle { font-size: 12px; color: var(--text2); }

.toolbar {
  display: flex; align-items: center; gap: 8px; padding: 10px 24px;
  background: var(--bg2); border-bottom: 1px solid var(--border); z-index: 10; position: relative;
}
.toolbar input {
  padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--bg); color: var(--text); font-size: 13px; width: 280px; outline: none;
}
.toolbar input:focus { border-color: var(--blue); }
.toolbar .tbtn {
  padding: 8px 16px; border-radius: 6px; font-size: 12px; font-weight: 600;
  cursor: pointer; border: none; background: var(--blue); color: var(--bg);
  text-transform: uppercase; letter-spacing: 0.5px;
}
.toolbar .tbtn:hover { background: #79c0ff; }
.toolbar .tbtn.secondary { background: var(--purple); }
.toolbar .tbtn.secondary:hover { background: #e2c5ff; }
.toolbar .info { margin-left: auto; font-size: 12px; color: var(--text2); display: flex; gap: 16px; }
.toolbar .info .iv { font-weight: 700; font-variant-numeric: tabular-nums; }
.toolbar .info .iv.blue { color: var(--blue); }
.toolbar .info .iv.orange { color: var(--orange); }
.toolbar .info .iv.green { color: var(--green); }
.toolbar .info .iv.red { color: var(--red); }

.main { display: flex; height: calc(100vh - 90px); }

.graph-container { flex: 1; position: relative; }
svg { width: 100%; height: 100%; }

.detail-panel {
  width: 340px; border-left: 1px solid var(--border); background: var(--bg2);
  overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px;
}
.detail-panel h3 { font-size: 12px; color: var(--text2); text-transform: uppercase; letter-spacing: 1px; }
.detail-panel .dp-empty { color: var(--text3); font-size: 13px; text-align: center; padding: 40px 0; }
.detail-card {
  padding: 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg);
  font-size: 12px; line-height: 1.6;
}
.detail-card .dc-title { font-size: 14px; font-weight: 600; color: #e6edf3; margin-bottom: 4px; }
.detail-card .dc-row { display: flex; justify-content: space-between; padding: 2px 0; }
.detail-card .dc-row .k { color: var(--text2); }
.detail-card .dc-row .v { font-weight: 600; font-variant-numeric: tabular-nums; }
.dc-badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 10px;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
}

.legend {
  display: flex; gap: 16px; flex-wrap: wrap; padding: 10px 24px;
  font-size: 11px; color: var(--text2); background: var(--bg2); border-top: 1px solid var(--border);
  position: absolute; bottom: 0; left: 0; right: 340px; z-index: 5;
}
.legend .li { display: flex; align-items: center; gap: 5px; }
.legend .dot { width: 10px; height: 10px; border-radius: 50%; }
.legend .dot.dashed { border: 2px dashed; background: transparent; width: 10px; height: 10px; }

.quick-picks { display: flex; flex-wrap: wrap; gap: 6px; }
.qp {
  padding: 4px 10px; border-radius: 12px; font-size: 11px;
  background: rgba(88,166,255,0.1); color: var(--blue); cursor: pointer;
  border: 1px solid transparent; transition: all 0.15s; max-width: 200px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.qp:hover { border-color: var(--blue); }

.loading-overlay {
  position: absolute; inset: 0; background: rgba(13,17,23,0.8);
  display: flex; align-items: center; justify-content: center;
  font-size: 14px; color: var(--text2); z-index: 20; display: none;
}
.loading-overlay.visible { display: flex; }
.loading-overlay::after { content: ''; display: inline-block; width: 16px; height: 16px; border: 2px solid var(--text3); border-top-color: var(--blue); border-radius: 50%; animation: spin 0.6s linear infinite; margin-left: 8px; }
@keyframes spin { to { transform: rotate(360deg); } }

.tooltip {
  position: absolute; padding: 8px 12px; border-radius: 6px;
  background: #1c2128; border: 1px solid var(--border); font-size: 11px;
  color: var(--text); pointer-events: none; z-index: 30; max-width: 240px;
  line-height: 1.5; display: none;
}
</style>
</head>
<body>
<div class="header">
  <h1>MongoDB Atlas PoC — Many-to-Many Relationship Graph</h1>
  <span class="subtitle">DUNS Entities &harr; Public Record Filings via Role Players</span>
</div>

<div class="toolbar">
  <input type="text" id="duns-input" placeholder="Enter a DUNS number..." />
  <button class="tbtn" onclick="searchDuns()">Explore</button>
  <button class="tbtn secondary" onclick="loadSamples()">Sample Entities</button>
  <div class="info">
    <span>Entities: <span class="iv blue" id="i-entities">0</span></span>
    <span>Filings: <span class="iv orange" id="i-filings">0</span></span>
    <span>Unmatched: <span class="iv red" id="i-unmatched">0</span></span>
    <span class="iv green" id="i-elapsed"></span>
  </div>
</div>

<div class="main">
  <div class="graph-container" id="graph-container">
    <svg id="graph-svg"></svg>
    <div class="loading-overlay" id="loading">Loading graph</div>
    <div class="tooltip" id="tooltip"></div>
  </div>
  <div class="detail-panel" id="detail-panel">
    <h3>Quick Picks</h3>
    <div class="quick-picks" id="quick-picks">
      <span class="qp" onclick="loadSamples()">Load entities from database...</span>
    </div>

    <h3>Selected Node</h3>
    <div class="dp-empty" id="dp-empty">Click a node in the graph to see details.</div>
    <div id="dp-content"></div>

    <h3 style="margin-top:12px;">How to Read This</h3>
    <div style="font-size:11px;color:var(--text2);line-height:1.7;">
      <p style="margin-bottom:6px;"><strong style="color:var(--blue);">Blue circles</strong> = DUNS entities (matched to entity universe)</p>
      <p style="margin-bottom:6px;"><strong style="color:var(--red);">Red dashed circles</strong> = Unmatched role players (no DUNS — needs research)</p>
      <p style="margin-bottom:6px;"><strong>Colored squares</strong> = Public record filings (suits, liens, etc.)</p>
      <p style="margin-bottom:6px;"><strong>Lines</strong> = Role player connections with role labels</p>
      <p style="margin-bottom:6px;">A filing connected to <strong>multiple entities</strong> = many-to-many.</p>
      <p>Click any node to see details. Double-click a DUNS node to re-center the graph on that entity.</p>
    </div>
  </div>
</div>

<div class="legend">
  <div class="li"><span class="dot" style="background:var(--blue)"></span> DUNS Entity (matched)</div>
  <div class="li"><span class="dot dashed" style="border-color:var(--red)"></span> Unmatched (no DUNS)</div>
  <div class="li"><span class="dot" style="background:var(--orange);border-radius:2px"></span> Suits</div>
  <div class="li"><span class="dot" style="background:var(--purple);border-radius:2px"></span> Liens</div>
  <div class="li"><span class="dot" style="background:var(--yellow);border-radius:2px"></span> Judgments</div>
  <div class="li"><span class="dot" style="background:var(--cyan);border-radius:2px"></span> UCCs</div>
  <div class="li"><span class="dot" style="background:var(--pink);border-radius:2px"></span> Bankruptcies</div>
  <div class="li" style="margin-left:auto;"><span style="color:var(--green)">Positive role</span> &mdash; <span style="color:var(--red)">Negative role</span></div>
</div>

<script>
const FILING_COLORS = {
  suits: '#f0883e', liens: '#d2a8ff', judgments: '#d29922',
  uccs: '#56d4dd', bankruptcies: '#f778ba'
};

const svg = d3.select('#graph-svg');
const container = document.getElementById('graph-container');
const tooltip = document.getElementById('tooltip');
let simulation, graphGroup;
let currentNodes = [], currentEdges = [];

function initGraph() {
  svg.selectAll('*').remove();

  svg.append('defs').append('marker')
    .attr('id', 'arrowhead').attr('viewBox', '0 -5 10 10')
    .attr('refX', 20).attr('refY', 0).attr('markerWidth', 6).attr('markerHeight', 6)
    .attr('orient', 'auto')
    .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#30363d');

  graphGroup = svg.append('g');

  const zoom = d3.zoom().scaleExtent([0.1, 4]).on('zoom', e => {
    graphGroup.attr('transform', e.transform);
  });
  svg.call(zoom);
}
initGraph();

function nodeRadius(d) {
  if (d.type === 'filing') return 14;
  if (d.is_origin) return 20;
  return 14;
}

function nodeColor(d) {
  if (d.type === 'unmatched') return 'transparent';
  if (d.type === 'filing') return FILING_COLORS[d.filing_type] || '#8b949e';
  return '#58a6ff';
}

function edgeColor(d) {
  if (d.polarity === 'negative') return '#f85149';
  if (d.polarity === 'positive') return '#3fb950';
  return '#484f58';
}

function renderGraph(data) {
  graphGroup.selectAll('*').remove();
  currentNodes = data.nodes;
  currentEdges = data.edges;

  const w = container.clientWidth;
  const h = container.clientHeight;

  const nodeMap = {};
  data.nodes.forEach(n => { nodeMap[n.id] = n; });

  const links = data.edges.map(e => ({
    source: e.source, target: e.target, role: e.role, polarity: e.polarity
  })).filter(e => nodeMap[e.source] && nodeMap[e.target]);

  simulation = d3.forceSimulation(data.nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(100))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(w / 2, h / 2))
    .force('collision', d3.forceCollide().radius(30));

  const link = graphGroup.append('g')
    .selectAll('line').data(links).join('line')
    .attr('stroke', edgeColor)
    .attr('stroke-width', 1.5)
    .attr('stroke-opacity', 0.6)
    .attr('marker-end', 'url(#arrowhead)');

  const linkLabel = graphGroup.append('g')
    .selectAll('text').data(links).join('text')
    .text(d => d.role)
    .attr('font-size', 9).attr('fill', '#484f58').attr('text-anchor', 'middle');

  const node = graphGroup.append('g')
    .selectAll('g').data(data.nodes).join('g')
    .call(d3.drag()
      .on('start', (e, d) => { if (!e.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
      .on('end', (e, d) => { if (!e.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
    );

  node.each(function(d) {
    const g = d3.select(this);
    if (d.type === 'filing') {
      const s = nodeRadius(d);
      g.append('rect').attr('x', -s).attr('y', -s).attr('width', s*2).attr('height', s*2)
        .attr('rx', 3).attr('fill', nodeColor(d)).attr('stroke', '#0d1117').attr('stroke-width', 2);
    } else if (d.type === 'unmatched') {
      g.append('circle').attr('r', nodeRadius(d))
        .attr('fill', 'transparent').attr('stroke', '#f85149')
        .attr('stroke-width', 2).attr('stroke-dasharray', '4,3');
    } else {
      g.append('circle').attr('r', nodeRadius(d))
        .attr('fill', nodeColor(d)).attr('stroke', d.is_origin ? '#e6edf3' : '#0d1117')
        .attr('stroke-width', d.is_origin ? 3 : 2);
    }

    g.append('text').text(d.label ? (d.label.length > 16 ? d.label.slice(0, 16) + '...' : d.label) : '')
      .attr('dy', nodeRadius(d) + 14).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('fill', '#8b949e');
  });

  node.on('mouseover', (e, d) => {
    let html = `<strong>${esc(d.label)}</strong>`;
    if (d.duns) html += `<br>DUNS: ${d.duns}`;
    if (d.address) html += `<br>${esc(d.address)}`;
    if (d.filing_type) html += `<br>Type: ${d.filing_type}`;
    if (d.filing_status) html += `<br>Status: ${d.filing_status}`;
    if (d.role_type) html += `<br>Role: ${d.role_type}`;
    if (!d.matched && d.type === 'unmatched') html += `<br><span style="color:#f85149">UNMATCHED — needs research</span>`;
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';
  })
  .on('mousemove', e => {
    tooltip.style.left = (e.pageX + 12) + 'px';
    tooltip.style.top = (e.pageY - 12) + 'px';
  })
  .on('mouseout', () => { tooltip.style.display = 'none'; })
  .on('click', (e, d) => { showDetail(d); })
  .on('dblclick', (e, d) => {
    if (d.type === 'duns' && d.duns) {
      document.getElementById('duns-input').value = d.duns;
      searchDuns();
    }
  });

  simulation.on('tick', () => {
    link.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    linkLabel
      .attr('x', d => (d.source.x + d.target.x) / 2)
      .attr('y', d => (d.source.y + d.target.y) / 2);
    node.attr('transform', d => `translate(${d.x},${d.y})`);
  });

  document.getElementById('i-entities').textContent = data.entity_count || data.nodes.filter(n => n.type === 'duns').length;
  document.getElementById('i-filings').textContent = data.filing_count || data.nodes.filter(n => n.type === 'filing').length;
  document.getElementById('i-unmatched').textContent = data.unmatched_count || data.nodes.filter(n => n.type === 'unmatched').length;
  document.getElementById('i-elapsed').textContent = data.elapsed_ms ? data.elapsed_ms + 'ms' : '';
}

function showDetail(d) {
  document.getElementById('dp-empty').style.display = 'none';
  const el = document.getElementById('dp-content');
  let html = '<div class="detail-card">';

  if (d.type === 'duns' || d.type === 'unmatched') {
    const badge = d.matched
      ? '<span class="dc-badge" style="background:rgba(63,185,80,0.2);color:#3fb950;">MATCHED</span>'
      : '<span class="dc-badge" style="background:rgba(248,81,73,0.2);color:#f85149;">UNMATCHED</span>';
    html += `<div class="dc-title">${esc(d.label)} ${badge}</div>`;
    if (d.duns) html += `<div class="dc-row"><span class="k">DUNS</span><span class="v" style="color:#58a6ff;font-family:monospace">${d.duns}</span></div>`;
    if (d.address) html += `<div class="dc-row"><span class="k">Address</span><span class="v">${esc(d.address)}</span></div>`;
    if (d.credit_score) html += `<div class="dc-row"><span class="k">Credit Score</span><span class="v">${d.credit_score}</span></div>`;
    if (d.status) html += `<div class="dc-row"><span class="k">Status</span><span class="v" style="color:${d.status==='active'?'#3fb950':'#f85149'}">${d.status}</span></div>`;
    if (d.role_type) html += `<div class="dc-row"><span class="k">Role</span><span class="v">${esc(d.role_type)}</span></div>`;

    const connections = currentEdges.filter(e => e.source.id === d.id || e.target.id === d.id || e.source === d.id || e.target === d.id);
    html += `<div class="dc-row"><span class="k">Connections</span><span class="v">${connections.length}</span></div>`;

    if (d.type === 'unmatched') {
      html += `<div style="margin-top:8px;padding:8px;border-radius:4px;background:rgba(248,81,73,0.08);color:#f85149;font-size:11px;">This role player has no DUNS number. A research agent would need to search and match this entity.</div>`;
    }

  } else if (d.type === 'filing') {
    const color = FILING_COLORS[d.filing_type] || '#8b949e';
    html += `<div class="dc-title" style="color:${color}">${esc(d.label)}</div>`;
    html += `<div class="dc-row"><span class="k">Type</span><span class="v" style="color:${color}">${esc(d.filing_type || '?')}</span></div>`;
    if (d.filing_id) html += `<div class="dc-row"><span class="k">Filing ID</span><span class="v" style="font-family:monospace">${esc(d.filing_id)}</span></div>`;
    if (d.filed_date) html += `<div class="dc-row"><span class="k">Filed</span><span class="v">${esc(d.filed_date)}</span></div>`;
    if (d.filing_status) html += `<div class="dc-row"><span class="k">Status</span><span class="v">${esc(d.filing_status)}</span></div>`;
    if (d.court) html += `<div class="dc-row"><span class="k">Court</span><span class="v">${esc(d.court)}</span></div>`;
    if (d.jurisdiction) html += `<div class="dc-row"><span class="k">Jurisdiction</span><span class="v">${esc(d.jurisdiction)}</span></div>`;
    if (d.amount) html += `<div class="dc-row"><span class="k">Amount</span><span class="v">$${Number(d.amount).toLocaleString()}</span></div>`;

    const roleNodes = currentEdges
      .filter(e => (e.target.id || e.target) === d.id)
      .map(e => {
        const srcId = e.source.id || e.source;
        const n = currentNodes.find(n => n.id === srcId);
        return { node: n, role: e.role, polarity: e.polarity };
      }).filter(x => x.node);

    if (roleNodes.length) {
      html += `<div style="margin-top:8px;font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:0.5px;">Role Players (${roleNodes.length})</div>`;
      roleNodes.forEach(rp => {
        const color = rp.polarity === 'negative' ? '#f85149' : rp.polarity === 'positive' ? '#3fb950' : '#8b949e';
        const matchBadge = rp.node.matched
          ? '<span style="color:#3fb950;font-size:10px;">matched</span>'
          : '<span style="color:#f85149;font-size:10px;">unmatched</span>';
        html += `<div style="padding:4px 0;border-bottom:1px solid #21262d;font-size:12px;">
          <span style="color:${color};font-weight:600;">${esc(rp.role)}</span>
          ${esc(rp.node.label)} ${matchBadge}
          ${rp.node.duns ? '<span style="color:#58a6ff;font-family:monospace;font-size:10px;">' + rp.node.duns + '</span>' : ''}
        </div>`;
      });
    }
  }

  html += '</div>';
  el.innerHTML = html;
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function searchDuns() {
  const duns = document.getElementById('duns-input').value.trim();
  if (!duns) return;
  const loading = document.getElementById('loading');
  loading.classList.add('visible');
  fetch(`/api/graph/duns/${encodeURIComponent(duns)}`)
    .then(r => r.json())
    .then(data => {
      loading.classList.remove('visible');
      if (data.error) { alert(data.error); return; }
      initGraph();
      renderGraph(data);
    })
    .catch(e => { loading.classList.remove('visible'); alert(e.message); });
}

function loadSamples() {
  const picks = document.getElementById('quick-picks');
  picks.innerHTML = '<span style="color:var(--text3);font-size:11px;">Loading...</span>';
  fetch('/api/sample-duns')
    .then(r => r.json())
    .then(data => {
      picks.innerHTML = '';
      (data.samples || []).forEach(s => {
        const el = document.createElement('span');
        el.className = 'qp';
        el.textContent = `${s.name.length > 22 ? s.name.slice(0,22) + '...' : s.name}`;
        el.title = `DUNS: ${s.duns}\nFound in: ${s.found_in}`;
        el.onclick = () => { document.getElementById('duns-input').value = s.duns; searchDuns(); };
        picks.appendChild(el);
      });
      const ref = document.createElement('span');
      ref.className = 'qp';
      ref.textContent = 'Refresh...';
      ref.onclick = loadSamples;
      picks.appendChild(ref);
    });
}

document.getElementById('duns-input').addEventListener('keydown', e => { if (e.key === 'Enter') searchDuns(); });
loadSamples();
</script>
</body>
</html>'''


@app.route('/')
def index():
    return render_template_string(GRAPH_HTML)


def main():
    parser = argparse.ArgumentParser(description='MongoDB Atlas PoC — Relationship Graph Explorer')
    parser.add_argument('--port', type=int, default=5080, help='Web server port (default: 5080)')
    parser.add_argument('--uri', help='MongoDB connection string (or set MONGODB_URI)')
    parser.add_argument('--db', help='Database name (or set MONGODB_DATABASE)')
    args = parser.parse_args()

    cfg = resolve_config()
    uri = args.uri or (cfg.connection_string if cfg else None)
    db_name = args.db or (cfg.database_name if cfg else None)

    if not uri or not db_name:
        print('Set MONGODB_URI and MONGODB_DATABASE env vars, or pass --uri and --db.')
        sys.exit(1)

    print(f'[relationship-graph] Database: {db_name}')
    print(f'[relationship-graph] Open http://localhost:{args.port} in your browser')
    app.run(host='0.0.0.0', port=args.port, threaded=True)


if __name__ == '__main__':
    main()
