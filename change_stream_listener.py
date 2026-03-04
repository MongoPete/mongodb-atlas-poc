#!/usr/bin/env python

'''
MongoDB Atlas PoC — Change Stream Listener (Downstream Publishing Proof)
================================================================
Purpose:
    Watches the `duns` (entity_core) collection via a MongoDB Change Stream
    and logs every insert/update/replace in real time. This is the live
    proof that MongoDB can push change events downstream — no polling needed.

    Run this ALONGSIDE locust_04_ops.py to demonstrate:
      - Name/address updates arrive instantly
      - Financial updates stream through
      - Bulk file changes show up as they happen
      - Only CHANGED records produce update events

    The listener also tracks the duns_audit collection to show
    publish_flag=true vs publish_flag=false events streaming in.

Usage:
    With env vars:
      export MONGODB_URI="mongodb+srv://..."
      export MONGODB_DATABASE="poc_demo"
      python change_stream_listener.py

    With explicit args:
      python change_stream_listener.py --uri "mongodb+srv://..." --db poc_demo

    Options:
      --collection duns         Watch a specific collection (default: duns)
      --audit                   Also watch the audit collection ({collection}_audit)
      --db-wide                 Watch ALL collections in the database
      --max-events 100          Stop after N events (default: unlimited)
      --quiet                   Suppress full document output, show summary only
'''

import os
import sys
import signal
import argparse
from datetime import datetime

import pymongo

from locust_db_config import resolve_config


# ------------------------------------------------------------------ #
#  ANSI colors for terminal output                                     #
# ------------------------------------------------------------------ #
class C:
    BOLD    = '\033[1m'
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    RED     = '\033[91m'
    CYAN    = '\033[96m'
    MAGENTA = '\033[95m'
    DIM     = '\033[2m'
    RESET   = '\033[0m'


OP_COLORS = {
    'insert':  C.GREEN,
    'update':  C.YELLOW,
    'replace': C.CYAN,
    'delete':  C.RED,
}


# ------------------------------------------------------------------ #
#  Stats tracker                                                       #
# ------------------------------------------------------------------ #
class Stats:
    def __init__(self):
        self.total = 0
        self.by_op = {}
        self.by_collection = {}
        self.audit_published = 0
        self.audit_suppressed = 0
        self.start_time = datetime.utcnow()

    def record(self, op_type, ns, is_audit=False, publish_flag=None):
        self.total += 1
        self.by_op[op_type] = self.by_op.get(op_type, 0) + 1
        self.by_collection[ns] = self.by_collection.get(ns, 0) + 1
        if is_audit and publish_flag is not None:
            if publish_flag:
                self.audit_published += 1
            else:
                self.audit_suppressed += 1

    def summary(self):
        elapsed = (datetime.utcnow() - self.start_time).total_seconds() or 1
        rate = self.total / elapsed
        lines = [
            '',
            f'{C.BOLD}{"=" * 60}',
            f'  Change Stream Summary',
            f'{"=" * 60}{C.RESET}',
            f'  Total events:    {C.BOLD}{self.total}{C.RESET}',
            f'  Duration:        {elapsed:.1f}s ({rate:.1f} events/sec)',
            f'  By operation:',
        ]
        for op, count in sorted(self.by_op.items()):
            color = OP_COLORS.get(op, '')
            lines.append(f'    {color}{op:12s}{C.RESET}  {count}')
        lines.append(f'  By collection:')
        for ns, count in sorted(self.by_collection.items()):
            lines.append(f'    {ns:30s}  {count}')
        if self.audit_published or self.audit_suppressed:
            total_audit = self.audit_published + self.audit_suppressed
            lines.append(f'  {C.BOLD}Audit breakdown:{C.RESET}')
            lines.append(f'    {C.GREEN}published (changed):   {self.audit_published}{C.RESET}')
            lines.append(f'    {C.DIM}suppressed (no change): {self.audit_suppressed}{C.RESET}')
            if total_audit:
                pct = self.audit_published / total_audit * 100
                lines.append(f'    change rate:            {pct:.1f}%')
        lines.append('')
        return '\n'.join(lines)


# ------------------------------------------------------------------ #
#  Event formatters                                                    #
# ------------------------------------------------------------------ #
def format_core_event(event, quiet=False):
    '''Format a change event from the core (duns) collection.'''
    op = event['operationType']
    color = OP_COLORS.get(op, '')
    ns = event['ns']['coll']
    ts = event.get('clusterTime', '')

    doc_key = event.get('documentKey', {})
    doc_id = doc_key.get('_id', '?')

    header = f'{C.DIM}[{ts}]{C.RESET} {color}{C.BOLD}{op.upper():8s}{C.RESET} {C.CYAN}{ns}{C.RESET} _id={doc_id}'

    if quiet:
        return header

    details = []
    if op == 'update' and 'updateDescription' in event:
        ud = event['updateDescription']
        updated = ud.get('updatedFields', {})
        removed = ud.get('removedFields', [])
        if updated:
            field_names = list(updated.keys())[:8]
            if 'current.name' in updated:
                details.append(f'  {C.YELLOW}name → {updated["current.name"]}{C.RESET}')
            if 'assessment.credit_score' in updated:
                details.append(f'  {C.MAGENTA}credit_score → {updated["assessment.credit_score"]}{C.RESET}')
            if 'financials.latest.total_revenue' in updated:
                rev = updated['financials.latest.total_revenue']
                details.append(f'  {C.GREEN}revenue → ${rev:,.0f}{C.RESET}')
            remaining = [f for f in field_names if f not in
                         ('current.name', 'assessment.credit_score', 'financials.latest.total_revenue')]
            if remaining:
                details.append(f'  {C.DIM}fields: {", ".join(remaining[:5])}{"..." if len(remaining) > 5 else ""}{C.RESET}')
            if removed:
                details.append(f'  {C.RED}removed: {", ".join(removed)}{C.RESET}')

    elif op == 'insert' and 'fullDocument' in event:
        doc = event['fullDocument']
        duns = doc.get('dunsNumber', '?')
        name = doc.get('current', {}).get('name', '?')
        details.append(f'  {C.GREEN}DUNS={duns}  name="{name}"{C.RESET}')

    return '\n'.join([header] + details) if details else header


def format_audit_event(event, quiet=False):
    '''Format a change event from the audit collection.'''
    op = event['operationType']
    ns = event['ns']['coll']
    ts = event.get('clusterTime', '')

    if op == 'insert' and 'fullDocument' in event:
        doc = event['fullDocument']
        duns = doc.get('dunsNumber', '?')
        action = doc.get('action', '?')
        publish = doc.get('publish_flag')
        user = doc.get('user', '?')

        if publish:
            flag_str = f'{C.GREEN}{C.BOLD}PUBLISH{C.RESET}'
        else:
            flag_str = f'{C.DIM}SUPPRESS{C.RESET}'

        header = f'{C.DIM}[{ts}]{C.RESET} {flag_str} {C.CYAN}{ns}{C.RESET} DUNS={duns} action={action}'

        if not quiet:
            change = doc.get('change_details', {})
            if change:
                parts = []
                if 'old_name' in change and 'new_name' in change:
                    parts.append(f'  {C.YELLOW}"{change["old_name"]}" → "{change["new_name"]}"{C.RESET}')
                elif 'name_changed' in change:
                    parts.append(f'  name_changed={change["name_changed"]}  addr_changed={change.get("address_changed")}')
                if parts:
                    return '\n'.join([header] + parts)

        return header

    color = OP_COLORS.get(op, '')
    return f'{C.DIM}[{ts}]{C.RESET} {color}{op.upper():8s}{C.RESET} {C.CYAN}{ns}{C.RESET}'


# ------------------------------------------------------------------ #
#  Main listener                                                       #
# ------------------------------------------------------------------ #
def run_listener(args):
    cfg = resolve_config()
    uri = args.uri or (cfg.connection_string if cfg else None)
    db_name = args.db or (cfg.database_name if cfg else None)
    collection = args.collection or (cfg.collection_name if cfg else 'duns')

    if not uri:
        print(f'{C.RED}No MongoDB URI. Set MONGODB_URI env var or pass --uri.{C.RESET}')
        sys.exit(1)
    if not db_name:
        print(f'{C.RED}No database name. Set MONGODB_DATABASE env var or pass --db.{C.RESET}')
        sys.exit(1)

    client = pymongo.MongoClient(uri, socketTimeoutMS=30000, connectTimeoutMS=5000)
    db = client[db_name]
    audit_collection = f'{collection}_audit'

    stats = Stats()

    def on_exit(sig, frame):
        print(stats.summary())
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    print(f'{C.BOLD}{"=" * 60}')
    print(f'  MongoDB Atlas PoC — Change Stream Listener')
    print(f'{"=" * 60}{C.RESET}')
    print(f'  Cluster:     {C.DIM}{uri[:60]}...{C.RESET}')
    print(f'  Database:    {C.CYAN}{db_name}{C.RESET}')

    if args.db_wide:
        print(f'  Watching:    {C.GREEN}ALL collections in {db_name}{C.RESET}')
    else:
        targets = [collection]
        if args.audit:
            targets.append(audit_collection)
        print(f'  Watching:    {C.GREEN}{", ".join(targets)}{C.RESET}')

    if args.max_events:
        print(f'  Max events:  {args.max_events}')
    print(f'  Quiet mode:  {"on" if args.quiet else "off"}')
    print(f'\n  {C.DIM}Press Ctrl+C to stop and see summary{C.RESET}')
    print(f'  {C.DIM}Waiting for changes...{C.RESET}\n')

    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'replace', 'delete']}}}]

    if args.db_wide:
        stream = db.watch(pipeline, full_document='updateLookup')
    elif args.audit:
        # Watch both collections via database-level stream with a namespace filter
        pipeline = [{'$match': {
            'operationType': {'$in': ['insert', 'update', 'replace', 'delete']},
            'ns.coll': {'$in': [collection, audit_collection]},
        }}]
        stream = db.watch(pipeline, full_document='updateLookup')
    else:
        stream = db[collection].watch(pipeline, full_document='updateLookup')

    try:
        for event in stream:
            ns_coll = event['ns']['coll']
            op_type = event['operationType']
            is_audit = ns_coll.endswith('_audit')

            publish_flag = None
            if is_audit and op_type == 'insert' and 'fullDocument' in event:
                publish_flag = event['fullDocument'].get('publish_flag')

            stats.record(op_type, ns_coll, is_audit=is_audit, publish_flag=publish_flag)

            if is_audit:
                print(format_audit_event(event, quiet=args.quiet))
            else:
                print(format_core_event(event, quiet=args.quiet))

            if args.max_events and stats.total >= args.max_events:
                print(f'\n{C.YELLOW}Reached max events ({args.max_events}). Stopping.{C.RESET}')
                break

    except pymongo.errors.PyMongoError as e:
        print(f'{C.RED}Change stream error: {e}{C.RESET}')
    finally:
        print(stats.summary())
        stream.close()
        client.close()


# ------------------------------------------------------------------ #
#  CLI                                                                 #
# ------------------------------------------------------------------ #
def main():
    parser = argparse.ArgumentParser(
        description='MongoDB Atlas PoC — Watch MongoDB Change Streams in real time',
    )
    parser.add_argument('--uri', help='MongoDB connection string (or set MONGODB_URI)')
    parser.add_argument('--db', help='Database name (or set MONGODB_DATABASE)')
    parser.add_argument('--collection', default=None, help='Collection to watch (default: duns)')
    parser.add_argument('--audit', action='store_true', help='Also watch the audit collection')
    parser.add_argument('--db-wide', action='store_true', help='Watch ALL collections in the database')
    parser.add_argument('--max-events', type=int, default=None, help='Stop after N events')
    parser.add_argument('--quiet', action='store_true', help='Summary lines only, no field details')
    args = parser.parse_args()
    run_listener(args)


if __name__ == '__main__':
    main()
