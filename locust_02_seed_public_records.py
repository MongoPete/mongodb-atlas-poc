#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

MongoDB Atlas PoC — Script 2 of 4: SEED PUBLIC RECORDS
==============================================
Purpose:
    Bulk insert synthetic public records (suits, liens, judgments,
    UCCs, bankruptcies) into their respective collections.
    Simulates loading the ~500M public records universe.

Data model (per Norm's March 2 spec):
    - Each filing has one or more role_players []
    - Each role_player has:
        names []     (1:many — primary, trade style, etc.)
        addresses [] (1:many — physical, mailing, etc.)
        polarity:    "positive" (filer) | "negative" (filed against)
        duns_number: linked DUNS or null (unmatched)
        matched:     bool — KEY flag for research agent unmatched search
    - Identical role_player construct across ALL filing types
      for model extensibility (Norm's explicit requirement)

Usage:
    With env vars (localized):
      export MONGODB_URI="mongodb+srv://..."
      export MONGODB_DATABASE="your_db"
      export MONGODB_COLLECTION="suits"
      export LOCUST_BULK_SIZE=200
      locust -f locust_02_seed_public_records.py --headless -u 10 -r 2 --run-time 10m

    Or with --host override (backward compatible):
      locust -f locust_02_seed_public_records.py \
             --host "mongodb+srv://<user>:<pass>@<cluster>|poc_demo|suits|200" \
             --headless -u 10 -r 2 --run-time 10m

Host format:  <connection_string>|<database>|<collection>|<bulk_size>
Supported collections:  suits | liens | judgments | uccs | bankruptcies

Run once per collection. Example:
    MONGODB_COLLECTION=suits locust -f locust_02_seed_public_records.py ...
    --host "...|poc_demo|liens|200"
'''

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import InsertOne
from locust import User, events, task, between

from locust_db_config import resolve_config
import time
import random
from datetime import datetime
from mimesis import Field
from mimesis.locales import Locale

# --- Mimesis Global ---
_ = Field(locale=Locale.EN)

# --- Role type definitions per filing type ---
ROLE_TYPES = {
    'suits':        [('plaintiff', 'positive'), ('defendant', 'negative'), ('attorney', 'positive'), ('judge', 'neutral')],
    'liens':        [('lien_holder', 'positive'), ('debtor', 'negative')],
    'judgments':    [('creditor', 'positive'), ('debtor', 'negative')],
    'uccs':         [('secured_party', 'positive'), ('debtor', 'negative')],
    'bankruptcies': [('trustee', 'positive'), ('debtor', 'negative'), ('attorney', 'positive'), ('courthouse', 'neutral')],
}

# Approx 25% of role players are unmatched (no DUNS number) — Norm's key requirement
UNMATCHED_RATE = 0.25

# --- Global State ---
_CLIENT = None
_REAL_DUNS_POOL = []   # populated on init from the `duns` collection


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    global _CLIENT
    if _CLIENT:
        print("Closing MongoDB connection...")
        _CLIENT.close()
        _CLIENT = None
        print("MongoDB connection closed.")


class SeedPublicRecordsUser(User):
    '''
    Bulk inserts synthetic public record filings.
    Each filing contains 1–4 role players with the shared role_player
    construct. A configurable percentage are left "unmatched" (no DUNS).
    '''
    wait_time = between(0.05, 0.2)
    client    = None
    coll      = None
    bulk_size = 200
    filing_type = 'suits'

    def __init__(self, parent):
        global _CLIENT
        super().__init__(parent)

        cfg = resolve_config(self.host)
        if not cfg or not cfg.connection_string:
            raise ValueError(
                "MongoDB connection required. Set MONGODB_URI (and optionally "
                "MONGODB_DATABASE, MONGODB_COLLECTION, LOCUST_BULK_SIZE) or use "
                "--host 'connection_string|db|collection|bulk_size'"
            )

        self.filing_type = cfg.collection_name
        if self.filing_type not in ROLE_TYPES:
            raise ValueError(
                f"Unsupported collection '{self.filing_type}'. "
                f"Must be one of: {list(ROLE_TYPES.keys())}"
            )

        self.bulk_size = cfg.bulk_size

        if _CLIENT is None:
            try:
                print(f"[seed_public_records] filing_type={self.filing_type}, bulk_size={self.bulk_size}")
                _CLIENT = pymongo.MongoClient(
                    cfg.connection_string,
                    w=1,
                    maxPoolSize=200,
                    socketTimeoutMS=10000,
                    connectTimeoutMS=5000,
                )
                db = _CLIENT[cfg.database_name]
                self.coll = db[cfg.collection_name]

                # Indexes: filing_id unique, DUNS lookup, matched flag for unmatched search
                self.coll.create_index([('filing_id', pymongo.ASCENDING)], unique=True, background=True)
                self.coll.create_index([('duns_numbers', pymongo.ASCENDING)], background=True)
                self.coll.create_index([('matched', pymongo.ASCENDING)], background=True)
                self.coll.create_index([('role_players.duns_number', pymongo.ASCENDING)], background=True)
                self.coll.create_index([('filed_date', pymongo.DESCENDING)], background=True)
                print(f"[seed_public_records] Indexes ensured for '{self.filing_type}'. Ready.")

                global _REAL_DUNS_POOL
                if not _REAL_DUNS_POOL:
                    try:
                        cursor = db['duns'].aggregate([
                            {'$sample': {'size': 500}},
                            {'$project': {'dunsNumber': 1, 'current.name': 1, '_id': 0}}
                        ])
                        _REAL_DUNS_POOL = [
                            (doc['dunsNumber'], doc.get('current', {}).get('name', ''))
                            for doc in cursor if doc.get('dunsNumber')
                        ]
                        print(f"[seed_public_records] Loaded {len(_REAL_DUNS_POOL)} real DUNS numbers from core collection for cross-linking.")
                    except Exception as pool_err:
                        print(f"[seed_public_records] Could not load real DUNS pool (will use random): {pool_err}")

            except Exception as e:
                print(f"[seed_public_records] Initialization failed: {e}")
                if self.environment:
                    self.environment.runner.quit()
                raise

        self.client = _CLIENT
        self.coll        = self.client[cfg.database_name][cfg.collection_name]
        self.bulk_size   = cfg.bulk_size
        self.filing_type = cfg.collection_name

    # ------------------------------------------------------------------ #
    #  Data Generation                                                     #
    # ------------------------------------------------------------------ #
    def _generate_role_player(self, role_type, polarity, filing_type):
        '''
        Generates a single role_player sub-document.

        Identical structure across all filing types (suits, liens, etc.)
        for extensibility — Norm's explicit architectural requirement.

        Names and addresses are arrays (1:many) because:
          - A company can have multiple trading names
          - A company can have a physical + mailing address
        '''
        is_matched  = random.random() > UNMATCHED_RATE
        real_name = None
        if is_matched and _REAL_DUNS_POOL:
            duns_number, real_name = random.choice(_REAL_DUNS_POOL)
        elif is_matched:
            duns_number = _('person.identifier', mask='#########')
        else:
            duns_number = None

        # Generate 1–2 names for this role player
        name_count = random.randint(1, 2)
        name_types = ['primary', 'trade_style', 'dba', 'former']
        primary_name = real_name if real_name else _('finance.company')
        names = [
            {
                'name_type': name_types[i] if i < len(name_types) else 'other',
                'name':      primary_name if i == 0 else _('finance.company') + ' ' + random.choice(['Inc', 'LLC', 'Co', 'Corp', '']),
            }
            for i in range(name_count)
        ]

        # Generate 1–2 addresses for this role player
        addr_count = random.randint(1, 2)
        addr_types = ['physical', 'mailing']
        addresses = [
            {
                'addr_type': addr_types[i] if i < len(addr_types) else 'other',
                'line1':     _('address.street_name'),
                'line2':     None,
                'city':      _('address.city'),
                'state':     _('address.state', abbr=True),
                'zip':       _('address.zip_code'),
                'country':   'US',
            }
            for i in range(addr_count)
        ]

        return {
            'role_type':   role_type,
            'polarity':    polarity,  # 'positive' (filer) | 'negative' (filed against) | 'neutral'
            'duns_number': duns_number,
            'matched':     is_matched,
            'names':       names,
            'addresses':   addresses,
            'meta': {
                'match_confidence': round(random.uniform(0.7, 1.0), 3) if is_matched else None,
                'match_method':     random.choice(['exact', 'fuzzy', 'manual']) if is_matched else None,
            },
        }

    def _generate_filing(self):
        '''
        Generates a single public record filing document.

        The top-level `matched` and `duns_numbers` fields are derived
        from the role_players array for efficient top-level querying.
        '''
        filing_type = self.filing_type
        roles       = ROLE_TYPES[filing_type]
        now         = datetime.utcnow()

        # Select 2–4 role types for this filing (always include the primary 2)
        selected_roles = roles[:2] + random.sample(roles[2:], min(random.randint(0, 2), len(roles) - 2))

        role_players = [
            self._generate_role_player(role_type, polarity, filing_type)
            for role_type, polarity in selected_roles
        ]

        # Derive top-level convenience fields from role_players
        all_duns    = [rp['duns_number'] for rp in role_players if rp['duns_number']]
        any_matched = any(rp['matched'] for rp in role_players)

        filing_id = _('person.identifier', mask='@@@@@@@@@@')

        return {
            '_id':          f"{filing_type.upper()}-{filing_id}",
            'filing_id':    filing_id,
            'filing_type':  filing_type,

            # Top-level matched flag — research agent uses this to find unmatched filings (AP-4)
            'matched':      any_matched,
            'duns_numbers': all_duns,  # all linked DUNS numbers for this filing

            # Filing metadata
            'filed_date':     _('datetime.date', start=2015, end=2025).isoformat(),
            'jurisdiction':   _('address.state', abbr=True),
            'court':          f"{_('address.city')} {random.choice(['District Court', 'Superior Court', 'County Court'])}",
            'case_number':    _('person.identifier', mask='##-CV-#####'),
            'filing_status':  random.choice(['open', 'closed', 'pending', 'appealed']),
            'amount':         _('numeric.integer_number', start=1000, end=10000000) if filing_type in ('suits', 'judgments') else None,

            # Role players array — THE core search target for AP-4
            'role_players':  role_players,

            'meta': {
                'version':       1,
                'source_system': random.choice(['Court_Feed', 'LexisNexis', 'Manual']),
                'created_date':  now,
                'last_modified': now,
            },
        }

    # ------------------------------------------------------------------ #
    #  Task                                                                #
    # ------------------------------------------------------------------ #
    @task
    def bulk_insert_public_records(self):
        '''
        Inserts bulk_size filing documents into the target collection.
        Uses InsertOne (not upsert) — this simulates streaming new filings.
        Duplicate _id errors are silently swallowed (expected on re-runs).
        '''
        task_name = f"bulk_insert_{self.filing_type}"
        tic = time.time()
        try:
            ops = [InsertOne(self._generate_filing()) for _ in range(self.bulk_size)]
            self.coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
            events.request.fire(
                request_type="mongodb",
                name=task_name,
                response_time=(time.time() - tic) * 1000,
                response_length=len(ops),
                exception=None,
            )
        except pymongo.errors.BulkWriteError as bwe:
            # Ignore duplicate key errors — expected on re-runs
            non_dupe_errors = [
                e for e in bwe.details.get('writeErrors', [])
                if e.get('code') != 11000
            ]
            exc = bwe if non_dupe_errors else None
            events.request.fire(
                request_type="mongodb",
                name=task_name,
                response_time=(time.time() - tic) * 1000,
                response_length=len(ops),
                exception=exc,
            )
        except Exception as e:
            events.request.fire(
                request_type="mongodb",
                name=task_name,
                response_time=(time.time() - tic) * 1000,
                response_length=0,
                exception=e,
            )
