#!/usr/bin/env python

'''
MongoDB Atlas PoC — Change Stream Live Dashboard
========================================
Web-based real-time feed of MongoDB change events.
Run alongside locust_04_ops.py to demo change detection to stakeholders.

Usage:
    export MONGODB_URI="mongodb+srv://..."
    export MONGODB_DATABASE="poc_demo"
    python change_stream_dashboard.py

    Then open http://localhost:5050 in your browser.

Options:
    --port 5050           Web server port (default: 5050)
    --collection duns     Collection to watch (default: duns)
'''

import os
import sys
import json
import queue
import argparse
import threading
from datetime import datetime

import pymongo
from flask import Flask, Response, render_template_string

from locust_db_config import resolve_config

app = Flask(__name__)

event_queue = queue.Queue(maxsize=500)

stats = {
    'total': 0,
    'updates': 0,
    'inserts': 0,
    'published': 0,
    'suppressed': 0,
    'name_changes': 0,
    'financial_updates': 0,
    'score_updates': 0,
    'bulk_changes': 0,
    'bulk_suppressed': 0,
    'cascades': 0,
    'start_time': None,
}
stats_lock = threading.Lock()

DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MongoDB Atlas PoC — Change Stream Live Feed</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #0d1117; color: #c9d1d9;
  }
  .header {
    background: linear-gradient(135deg, #161b22 0%, #0d1117 100%);
    border-bottom: 1px solid #30363d;
    padding: 16px 32px; display: flex; align-items: center; gap: 16px;
  }
  .header h1 { font-size: 20px; font-weight: 600; color: #e6edf3; }
  .header .subtitle { font-size: 13px; color: #8b949e; }
  .header .live-dot {
    width: 10px; height: 10px; border-radius: 50%; background: #3fb950;
    animation: pulse 2s infinite; margin-left: auto;
  }
  .header .live-label { font-size: 12px; color: #3fb950; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

  .stats-bar {
    display: flex; gap: 0; border-bottom: 1px solid #30363d;
    background: #161b22; padding: 0 16px; overflow-x: auto;
  }
  .stat {
    padding: 12px 18px; text-align: center; min-width: 90px;
    border-right: 1px solid #30363d; cursor: pointer; transition: background 0.2s;
    position: relative;
  }
  .stat:last-child { border-right: none; }
  .stat:hover { background: rgba(255,255,255,0.03); }
  .stat.active-filter { background: rgba(88,166,255,0.1); }
  .stat.active-filter::after {
    content: ''; position: absolute; bottom: 0; left: 20%; right: 20%;
    height: 2px; background: #58a6ff; border-radius: 1px;
  }
  .stat .val { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .stat .label { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }
  .stat.publish .val { color: #3fb950; }
  .stat.suppress .val { color: #8b949e; }
  .stat.change .val { color: #d29922; }
  .stat.total .val { color: #58a6ff; }
  .stat.rate .val { color: #bc8cff; font-size: 18px; }
  .stat.bulk .val { color: #f0883e; }
  .stat.cascade .val { color: #38ada9; }
  .stat.financial .val { color: #79c0ff; }
  .stat.score .val { color: #d2a8ff; }

  .filter-bar {
    display: flex; align-items: center; gap: 6px; padding: 8px 16px;
    background: #161b22; border-bottom: 1px solid #30363d; flex-wrap: wrap;
  }
  .filter-bar .filter-label { font-size: 11px; color: #484f58; text-transform: uppercase; letter-spacing: 1px; margin-right: 4px; }
  .fbtn {
    padding: 4px 12px; border-radius: 14px; font-size: 11px; font-weight: 600;
    border: 1px solid #30363d; background: transparent; color: #8b949e;
    cursor: pointer; transition: all 0.15s; text-transform: uppercase; letter-spacing: 0.3px;
  }
  .fbtn:hover { border-color: #58a6ff; color: #c9d1d9; }
  .fbtn.active { background: #58a6ff; color: #0d1117; border-color: #58a6ff; }
  .fbtn.active.f-publish { background: #3fb950; border-color: #3fb950; }
  .fbtn.active.f-suppress { background: #484f58; border-color: #484f58; color: #e6edf3; }
  .fbtn.active.f-name { background: #d29922; border-color: #d29922; }
  .fbtn.active.f-financial { background: #79c0ff; border-color: #79c0ff; color: #0d1117; }
  .fbtn.active.f-score { background: #d2a8ff; border-color: #d2a8ff; color: #0d1117; }
  .fbtn.active.f-bulk { background: #f0883e; border-color: #f0883e; color: #0d1117; }
  .fbtn.active.f-cascade { background: #38ada9; border-color: #38ada9; color: #0d1117; }
  .fbtn.active.f-update { background: #d29922; border-color: #d29922; color: #0d1117; }
  .fbtn .fcount { margin-left: 4px; opacity: 0.7; }
  .filter-clear {
    margin-left: auto; font-size: 11px; color: #484f58; cursor: pointer;
    text-decoration: underline; display: none;
  }
  .filter-clear.visible { display: inline; }
  .filter-clear:hover { color: #c9d1d9; }

  .sort-toggle {
    padding: 4px 10px; border-radius: 4px; font-size: 11px;
    border: 1px solid #30363d; background: transparent; color: #8b949e;
    cursor: pointer; margin-left: 8px;
  }
  .sort-toggle:hover { border-color: #58a6ff; color: #c9d1d9; }

  .feed-container { display: flex; height: calc(100vh - 160px); }

  .feed {
    flex: 1; overflow-y: auto; padding: 8px 16px;
    display: flex; flex-direction: column; gap: 3px;
  }
  .feed::-webkit-scrollbar { width: 6px; }
  .feed::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }

  .event {
    padding: 8px 12px; border-radius: 6px; font-size: 13px;
    border-left: 3px solid transparent; animation: slideIn 0.2s ease-out;
    line-height: 1.5; cursor: pointer; transition: background 0.15s;
  }
  .event:hover { filter: brightness(1.2); }
  .event.hidden { display: none; }
  @keyframes slideIn { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; transform: translateY(0); } }

  .event.publish { background: rgba(63, 185, 80, 0.08); border-left-color: #3fb950; }
  .event.suppress { background: rgba(139, 148, 158, 0.06); border-left-color: #484f58; }
  .event.update { background: rgba(210, 153, 34, 0.08); border-left-color: #d29922; }
  .event.insert { background: rgba(88, 166, 255, 0.08); border-left-color: #58a6ff; }

  .event .ev-header { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }
  .event .ts { color: #484f58; font-size: 11px; font-family: monospace; }
  .event .badge {
    display: inline-block; padding: 1px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .badge.pub { background: rgba(63,185,80,0.2); color: #3fb950; }
  .badge.sup { background: rgba(139,148,158,0.15); color: #8b949e; }
  .badge.upd { background: rgba(210,153,34,0.2); color: #d29922; }
  .badge.ins { background: rgba(88,166,255,0.2); color: #58a6ff; }

  .event .bucket-tag {
    display: inline-block; padding: 1px 6px; border-radius: 3px;
    font-size: 10px; font-weight: 500; text-transform: uppercase;
    letter-spacing: 0.3px; margin-left: 2px;
  }
  .bucket-tag.bt-name { background: rgba(210,153,34,0.15); color: #d29922; }
  .bucket-tag.bt-financial { background: rgba(121,192,255,0.15); color: #79c0ff; }
  .bucket-tag.bt-score { background: rgba(210,168,255,0.15); color: #d2a8ff; }
  .bucket-tag.bt-bulk { background: rgba(240,136,62,0.15); color: #f0883e; }
  .badge.cascade { background: rgba(56,173,169,0.2); color: #38ada9; }
  .bucket-tag.bt-cascade { background: rgba(56,173,169,0.15); color: #38ada9; }
  .ev.cascade { border-left-color: #38ada9; }

  .event .duns { color: #58a6ff; font-weight: 600; font-family: monospace; }
  .event .action { color: #8b949e; font-size: 12px; }
  .event .detail { color: #c9d1d9; margin-top: 4px; }
  .event .old-val { color: #f85149; text-decoration: line-through; }
  .event .new-val { color: #3fb950; font-weight: 600; }
  .event .arrow { color: #484f58; margin: 0 4px; }
  .event .coll { color: #bc8cff; font-size: 11px; }
  .event .fields { color: #484f58; font-size: 11px; margin-top: 2px; }

  .event .expand-detail {
    display: none; margin-top: 8px; padding: 8px 10px;
    background: rgba(0,0,0,0.3); border-radius: 4px; font-size: 12px;
    font-family: monospace; line-height: 1.6; white-space: pre-wrap;
    color: #8b949e; border: 1px solid #21262d;
  }
  .event.expanded .expand-detail { display: block; }

  .sidebar {
    width: 280px; border-left: 1px solid #30363d; padding: 16px;
    background: #161b22; overflow-y: auto;
  }
  .sidebar h3 { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }
  .breakdown { margin-bottom: 20px; }
  .breakdown .row {
    display: flex; justify-content: space-between; padding: 5px 0;
    border-bottom: 1px solid #21262d; font-size: 13px;
  }
  .breakdown .row .k { color: #8b949e; }
  .breakdown .row .v { font-weight: 600; font-variant-numeric: tabular-nums; }

  .change-rate-bar {
    height: 8px; background: #21262d; border-radius: 4px;
    overflow: hidden; margin-top: 8px;
  }
  .change-rate-fill {
    height: 100%; background: linear-gradient(90deg, #3fb950, #d29922);
    border-radius: 4px; transition: width 0.5s ease;
  }
  .empty-state {
    display: flex; align-items: center; justify-content: center;
    height: 100%; color: #484f58; font-size: 15px;
  }
</style>
</head>
<body>
<div class="header">
  <h1>MongoDB Atlas PoC — Change Stream Live Feed</h1>
  <span class="subtitle">MongoDB Change Detection &rarr; Downstream Publishing</span>
  <div class="live-dot"></div>
  <span class="live-label">Live</span>
</div>

<div class="stats-bar">
  <div class="stat total" data-filter="all" title="Show all events"><div class="val" id="s-total">0</div><div class="label">Total Events</div></div>
  <div class="stat publish" data-filter="publish" title="Filter: Published"><div class="val" id="s-pub">0</div><div class="label">Published</div></div>
  <div class="stat suppress" data-filter="suppress" title="Filter: Suppressed"><div class="val" id="s-sup">0</div><div class="label">Suppressed</div></div>
  <div class="stat change" data-filter="name" title="Filter: Name Changes"><div class="val" id="s-name">0</div><div class="label">Name Changes</div></div>
  <div class="stat financial" data-filter="financial" title="Filter: Financial"><div class="val" id="s-fin">0</div><div class="label">Financial</div></div>
  <div class="stat score" data-filter="score" title="Filter: Score Updates"><div class="val" id="s-score">0</div><div class="label">Score Updates</div></div>
  <div class="stat bulk" data-filter="bulk" title="Filter: Bulk File"><div class="val" id="s-bulk">0</div><div class="label">Bulk File</div></div>
  <div class="stat cascade" data-filter="cascade" title="Filter: Cascade Updates"><div class="val" id="s-cascade">0</div><div class="label">Cascades</div></div>
  <div class="stat rate"><div class="val" id="s-rate">&mdash;</div><div class="label">Change Rate</div></div>
</div>

<div class="filter-bar">
  <span class="filter-label">Filter:</span>
  <button class="fbtn active" data-bucket="all">All</button>
  <button class="fbtn f-publish" data-bucket="publish">Publish</button>
  <button class="fbtn f-suppress" data-bucket="suppress">Suppress</button>
  <button class="fbtn f-name" data-bucket="name">Name Changes</button>
  <button class="fbtn f-financial" data-bucket="financial">Financial</button>
  <button class="fbtn f-score" data-bucket="score">Score</button>
  <button class="fbtn f-bulk" data-bucket="bulk">Bulk File</button>
  <button class="fbtn f-cascade" data-bucket="cascade">Cascade</button>
  <button class="fbtn f-update" data-bucket="update">Core Updates</button>
  <span class="filter-clear" id="filter-clear" onclick="clearFilters()">Clear filters</span>
  <button class="sort-toggle" id="sort-toggle" onclick="toggleSort()" title="Toggle newest/oldest first">Newest First</button>
</div>

<div class="feed-container">
  <div class="feed" id="feed">
    <div class="empty-state" id="empty">Waiting for change events...</div>
  </div>
  <div class="sidebar">
    <h3>Audit Breakdown</h3>
    <div class="breakdown">
      <div class="row"><span class="k">Bulk file changes</span><span class="v" id="sb-bulk" style="color:#f0883e">0</span></div>
      <div class="row"><span class="k">Bulk file suppressed</span><span class="v" id="sb-bulksup" style="color:#8b949e">0</span></div>
      <div class="row"><span class="k">Name/addr updates</span><span class="v" id="sb-name" style="color:#d29922">0</span></div>
      <div class="row"><span class="k">Financial updates</span><span class="v" id="sb-fin" style="color:#79c0ff">0</span></div>
      <div class="row"><span class="k">Score updates</span><span class="v" id="sb-score" style="color:#d2a8ff">0</span></div>
    </div>
    <h3>Change Rate</h3>
    <div style="font-size:13px;color:#8b949e;margin-bottom:6px;">Published vs Suppressed</div>
    <div class="change-rate-bar">
      <div class="change-rate-fill" id="rate-bar" style="width:0%"></div>
    </div>
    <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:11px;color:#484f58;">
      <span>0% changed</span>
      <span id="rate-label">&mdash;</span>
    </div>

    <h3 style="margin-top:20px;">Legend</h3>
    <div style="font-size:12px;color:#8b949e;line-height:1.7;">
      <p style="margin-bottom:6px;"><span class="badge pub" style="font-size:10px;">PUBLISH</span> Record changed. Pushed downstream.</p>
      <p style="margin-bottom:6px;"><span class="badge sup" style="font-size:10px;">SUPPRESS</span> No change. Not republished.</p>
      <p style="margin-bottom:6px;"><span class="badge upd" style="font-size:10px;">UPDATE</span> Core document modified.</p>
      <p style="margin-bottom:6px;"><span class="badge ins" style="font-size:10px;">INSERT</span> New document created.</p>
      <p style="margin-top:10px;font-size:11px;color:#484f58;">Click any event to expand details.</p>
    </div>
  </div>
</div>

<script>
const feed = document.getElementById('feed');
const empty = document.getElementById('empty');
const MAX_EVENTS = 300;
let activeFilter = 'all';
let sortNewest = true;

/* ---- Filter buttons ---- */
document.querySelectorAll('.fbtn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.fbtn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    activeFilter = btn.dataset.bucket;
    document.getElementById('filter-clear').classList.toggle('visible', activeFilter !== 'all');
    applyFilter();
  });
});

document.querySelectorAll('.stat[data-filter]').forEach(stat => {
  stat.addEventListener('click', () => {
    const f = stat.dataset.filter;
    activeFilter = f;
    document.querySelectorAll('.fbtn').forEach(b => {
      b.classList.toggle('active', b.dataset.bucket === f);
    });
    document.querySelectorAll('.stat[data-filter]').forEach(s => s.classList.remove('active-filter'));
    if (f !== 'all') stat.classList.add('active-filter');
    document.getElementById('filter-clear').classList.toggle('visible', f !== 'all');
    applyFilter();
  });
});

function clearFilters() {
  activeFilter = 'all';
  document.querySelectorAll('.fbtn').forEach(b => {
    b.classList.toggle('active', b.dataset.bucket === 'all');
  });
  document.querySelectorAll('.stat[data-filter]').forEach(s => s.classList.remove('active-filter'));
  document.getElementById('filter-clear').classList.remove('visible');
  applyFilter();
}

function applyFilter() {
  feed.querySelectorAll('.event').forEach(ev => {
    if (activeFilter === 'all') {
      ev.classList.remove('hidden');
    } else {
      const buckets = (ev.dataset.buckets || '').split(',');
      ev.classList.toggle('hidden', !buckets.includes(activeFilter));
    }
  });
}

function toggleSort() {
  sortNewest = !sortNewest;
  document.getElementById('sort-toggle').textContent = sortNewest ? 'Newest First' : 'Oldest First';
  const events = Array.from(feed.querySelectorAll('.event'));
  events.reverse().forEach(ev => feed.appendChild(ev));
}

/* ---- Click to expand ---- */
feed.addEventListener('click', function(e) {
  const ev = e.target.closest('.event');
  if (ev) ev.classList.toggle('expanded');
});

/* ---- SSE stream ---- */
const evtSource = new EventSource('/stream');
evtSource.onmessage = function(e) {
  const d = JSON.parse(e.data);

  if (d.type !== 'event') {
    if (d.type === 'stats') {
      document.getElementById('s-total').textContent = d.total;
      document.getElementById('s-pub').textContent = d.published;
      document.getElementById('s-sup').textContent = d.suppressed;
      document.getElementById('s-name').textContent = d.name_changes;
      document.getElementById('s-fin').textContent = d.financial_updates;
      document.getElementById('s-score').textContent = d.score_updates;
      document.getElementById('s-bulk').textContent = d.bulk_changes + d.bulk_suppressed;
      if (d.cascades !== undefined) document.getElementById('s-cascade').textContent = d.cascades;
      document.getElementById('sb-bulk').textContent = d.bulk_changes;
      document.getElementById('sb-bulksup').textContent = d.bulk_suppressed;
      document.getElementById('sb-name').textContent = d.name_changes;
      document.getElementById('sb-fin').textContent = d.financial_updates;
      document.getElementById('sb-score').textContent = d.score_updates;
      const total_audit = d.published + d.suppressed;
      if (total_audit > 0) {
        const pct = (d.published / total_audit * 100).toFixed(1);
        document.getElementById('s-rate').textContent = pct + '%';
        document.getElementById('rate-bar').style.width = pct + '%';
        document.getElementById('rate-label').textContent = pct + '% changed';
      }
    }
    return;
  }

  if (empty && empty.parentNode) empty.remove();

  const div = document.createElement('div');
  div.className = 'event ' + d.css_class;
  div.dataset.buckets = (d.buckets || []).join(',');
  div.innerHTML = d.html;

  if (activeFilter !== 'all') {
    const buckets = d.buckets || [];
    if (!buckets.includes(activeFilter)) div.classList.add('hidden');
  }

  const atTop = feed.scrollTop < 50;
  const prevHeight = feed.scrollHeight;

  if (sortNewest) {
    feed.insertBefore(div, feed.firstChild);
  } else {
    feed.appendChild(div);
  }

  if (sortNewest && !atTop) {
    feed.scrollTop += (feed.scrollHeight - prevHeight);
  } else if (sortNewest && atTop) {
    feed.scrollTop = 0;
  }

  while (feed.querySelectorAll('.event').length > MAX_EVENTS) {
    const target = sortNewest ? feed.lastElementChild : feed.firstElementChild;
    if (target && target.classList.contains('event')) feed.removeChild(target);
    else break;
  }
};
</script>
</body>
</html>'''


@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route('/stream')
def stream():
    def generate():
        while True:
            try:
                data = event_queue.get(timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


def _escape_html(s):
    '''Minimal HTML escaping for user-generated content.'''
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def _build_raw_detail(event):
    '''Build a compact JSON string of interesting fields for the expand-detail panel.'''
    parts = {}
    op = event.get('operationType', '')
    parts['operation'] = op
    parts['collection'] = event.get('ns', {}).get('coll', '')
    dk = event.get('documentKey', {})
    if dk:
        parts['documentKey'] = str(dk.get('_id', ''))

    if op == 'insert' and 'fullDocument' in event:
        doc = event['fullDocument']
        for key in ('dunsNumber', 'action', 'publish_flag', 'change_details',
                     'current', 'assessment', 'financials'):
            if key in doc:
                val = doc[key]
                if isinstance(val, dict):
                    parts[key] = {k: str(v) for k, v in list(val.items())[:8]}
                else:
                    parts[key] = str(val)

    if 'updateDescription' in event:
        ud = event['updateDescription'].get('updatedFields', {})
        if ud:
            parts['updatedFields'] = {k: str(v) for k, v in list(ud.items())[:10]}
        removed = event['updateDescription'].get('removedFields', [])
        if removed:
            parts['removedFields'] = removed[:10]

    try:
        return json.dumps(parts, indent=2, default=str)
    except Exception:
        return '{}'


def format_event_html(event):
    '''Convert a MongoDB change event into an HTML snippet + CSS class + buckets for the dashboard.'''
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

        with stats_lock:
            if publish:
                stats['published'] += 1
                if 'BULK_FILE' in action_raw:
                    stats['bulk_changes'] += 1
            else:
                stats['suppressed'] += 1
                if 'BULK_FILE' in action_raw:
                    stats['bulk_suppressed'] += 1

            if 'NAME' in action_raw and 'BULK' not in action_raw:
                stats['name_changes'] += 1
            elif 'FINANCIAL' in action_raw:
                stats['financial_updates'] += 1
            elif 'SCORE' in action_raw:
                stats['score_updates'] += 1
            elif 'BULK_FILE_NAME' in action_raw:
                stats['name_changes'] += 1

        if publish:
            badge = '<span class="badge pub">PUBLISH</span>'
            css_class = 'publish'
            buckets.append('publish')
        else:
            badge = '<span class="badge sup">SUPPRESS</span>'
            css_class = 'suppress'
            buckets.append('suppress')

        if 'BULK_FILE' in action_raw or 'BULK' in action_raw:
            buckets.append('bulk')
        if 'NAME' in action_raw:
            buckets.append('name')
        if 'FINANCIAL' in action_raw:
            buckets.append('financial')
        if 'SCORE' in action_raw:
            buckets.append('score')

        bucket_tags = ''.join(_bucket_tag(b) for b in buckets if b not in ('publish', 'suppress'))

        detail_html = ''
        if change.get('old_name') and change.get('new_name'):
            detail_html = (f'<div class="detail">'
                           f'<span class="old-val">{_escape_html(change["old_name"])}</span>'
                           f'<span class="arrow">&rarr;</span>'
                           f'<span class="new-val">{_escape_html(change["new_name"])}</span></div>')
        elif change.get('name_changed'):
            old = _escape_html(change.get('old_name', '?'))
            new = _escape_html(change.get('new_name', '?'))
            detail_html = (f'<div class="detail">'
                           f'<span class="old-val">{old}</span>'
                           f'<span class="arrow">&rarr;</span>'
                           f'<span class="new-val">{new}</span></div>')

        html = (f'<div class="ev-header">'
                f'<span class="ts">{ts}</span> {badge} {bucket_tags} '
                f'<span class="coll">{ns_coll}</span> '
                f'DUNS=<span class="duns">{duns}</span> '
                f'<span class="action">{action}</span>'
                f'</div>'
                f'{detail_html}'
                f'<div class="expand-detail">{_escape_html(raw_detail)}</div>')

        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    if op == 'update':
        css_class = 'update'
        badge = '<span class="badge upd">UPDATE</span>'
        buckets.append('update')

        detail_html = ''
        if 'updateDescription' in event:
            fields = list(event['updateDescription'].get('updatedFields', {}).keys())
            ud = event['updateDescription'].get('updatedFields', {})

            if 'current.name' in ud:
                with stats_lock:
                    stats['name_changes'] += 1
                buckets.append('name')
                detail_html = f'<div class="detail"><span class="new-val">name &rarr; {_escape_html(ud["current.name"])}</span></div>'
            elif 'financials.latest.total_revenue' in ud:
                rev = ud['financials.latest.total_revenue']
                buckets.append('financial')
                detail_html = f'<div class="detail">revenue &rarr; ${rev:,.0f}</div>'
            elif 'assessment.credit_score' in ud:
                buckets.append('score')
                detail_html = f'<div class="detail">credit_score &rarr; {_escape_html(ud["assessment.credit_score"])}</div>'

            short_fields = [f for f in fields[:4]]
            if short_fields:
                detail_html += f'<div class="fields">{", ".join(short_fields)}{"..." if len(fields) > 4 else ""}</div>'

        bucket_tags = ''.join(_bucket_tag(b) for b in buckets if b != 'update')
        duns_str = doc_id.replace('CORE-', '') if doc_id.startswith('CORE-') else doc_id

        html = (f'<div class="ev-header">'
                f'<span class="ts">{ts}</span> {badge} {bucket_tags} '
                f'<span class="coll">{ns_coll}</span> '
                f'DUNS=<span class="duns">{_escape_html(duns_str)}</span>'
                f'</div>'
                f'{detail_html}'
                f'<div class="expand-detail">{_escape_html(raw_detail)}</div>')

        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    if op == 'insert' and not is_audit:
        css_class = 'insert'
        badge = '<span class="badge ins">INSERT</span>'
        buckets.append('publish')
        detail_html = ''
        if 'fullDocument' in event:
            doc = event['fullDocument']
            duns = _escape_html(doc.get('dunsNumber', '?'))
            name = _escape_html(doc.get('current', {}).get('name', ''))
            if name:
                detail_html = f'<div class="detail">{name}</div>'

        html = (f'<div class="ev-header">'
                f'<span class="ts">{ts}</span> {badge} '
                f'<span class="coll">{ns_coll}</span> '
                f'DUNS=<span class="duns">{_escape_html(doc_id)}</span>'
                f'</div>'
                f'{detail_html}'
                f'<div class="expand-detail">{_escape_html(raw_detail)}</div>')
        return {'html': html, 'css_class': css_class, 'buckets': buckets, 'type': 'event'}

    return None


def _bucket_tag(bucket):
    '''Return a small colored tag HTML span for a bucket category.'''
    labels = {
        'name': ('NAME', 'bt-name'),
        'financial': ('FINANCIAL', 'bt-financial'),
        'score': ('SCORE', 'bt-score'),
        'bulk': ('BULK', 'bt-bulk'),
        'cascade': ('CASCADE', 'bt-cascade'),
    }
    if bucket in labels:
        text, cls = labels[bucket]
        return f'<span class="bucket-tag {cls}">{text}</span>'
    return ''


def watch_changes(uri, db_name, collection_name):
    '''Background thread: watches MongoDB change stream and pushes events to the queue.'''
    client = pymongo.MongoClient(uri, socketTimeoutMS=30000, connectTimeoutMS=5000)
    db = client[db_name]
    audit_name = f'{collection_name}_audit'

    pipeline = [{'$match': {
        'operationType': {'$in': ['insert', 'update', 'replace', 'delete']},
        'ns.coll': {'$in': [collection_name, audit_name]},
    }}]

    print(f'[dashboard] Watching {db_name}.{collection_name} + {audit_name}')

    with stats_lock:
        stats['start_time'] = datetime.utcnow().isoformat()

    stream = db.watch(pipeline, full_document='updateLookup')
    try:
        for event in stream:
            with stats_lock:
                stats['total'] += 1

            formatted = format_event_html(event)
            if formatted:
                try:
                    event_queue.put_nowait(formatted)
                except queue.Full:
                    try:
                        event_queue.get_nowait()
                    except queue.Empty:
                        pass
                    event_queue.put_nowait(formatted)

            with stats_lock:
                snap = dict(stats)
            snap['type'] = 'stats'
            try:
                event_queue.put_nowait(snap)
            except queue.Full:
                pass

    except pymongo.errors.PyMongoError as e:
        print(f'[dashboard] Change stream error: {e}')
    finally:
        stream.close()
        client.close()


def main():
    parser = argparse.ArgumentParser(description='MongoDB Atlas PoC — Change Stream Live Dashboard')
    parser.add_argument('--port', type=int, default=5050, help='Web server port (default: 5050)')
    parser.add_argument('--uri', help='MongoDB connection string (or set MONGODB_URI)')
    parser.add_argument('--db', help='Database name (or set MONGODB_DATABASE)')
    parser.add_argument('--collection', default=None, help='Collection to watch (default: duns)')
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

    print(f'[dashboard] Open http://localhost:{args.port} in your browser')
    app.run(host='0.0.0.0', port=args.port, threaded=True)


if __name__ == '__main__':
    main()
