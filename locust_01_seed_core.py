#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

MongoDB Atlas PoC — Script 1 of 4: SEED ENTITY CORE
===========================================
Purpose:
    Bulk upsert synthetic DUNS entity records into the `entity_core`
    collection. Simulates the initial seeding of 10M–130M core records.

Data model:
    - DUNS number as the primary key (coat hanger)
    - Collapsed: names, addresses, legal structure, employees, financials,
      assessment scores in a single document (per Norm's agreed model)
    - former_names / former_addresses arrays pre-seeded as empty
      (populated by locust_04_ops.py)

Usage:
    With env vars (localized):
      export MONGODB_URI="mongodb+srv://..."
      export MONGODB_DATABASE="your_db"
      export MONGODB_COLLECTION="duns"
      export LOCUST_BULK_SIZE=500
      locust -f locust_01_seed_core.py --headless -u 10 -r 2 --run-time 10m

    Or with --host override (backward compatible):
      locust -f locust_01_seed_core.py \
             --host "mongodb+srv://<user>:<pass>@<cluster>|poc_demo|duns|500" \
             --headless -u 10 -r 2 --run-time 10m

Host format:  <connection_string>|<database>|<collection>|<bulk_size>
'''

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import UpdateOne
from locust import User, events, task, between

from locust_db_config import resolve_config
import time
import random
from datetime import datetime
from mimesis import Field
from mimesis.locales import Locale

# --- Mimesis Global ---
_ = Field(locale=Locale.EN)

# --- Global State ---
_CLIENT = None


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    global _CLIENT
    if _CLIENT:
        print("Closing MongoDB connection...")
        _CLIENT.close()
        _CLIENT = None
        print("MongoDB connection closed.")


class SeedCoreUser(User):
    '''
    Performs high-throughput bulk upserts into entity_core.
    Each task generates `bulk_size` documents and issues a single
    bulk_write with ordered=False for maximum throughput.
    '''
    wait_time = between(0.05, 0.2)
    client = None
    coll   = None
    bulk_size = 500

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

        self.bulk_size = cfg.bulk_size

        if _CLIENT is None:
            try:
                print(f"[seed_core] Connecting to MongoDB... bulk_size={self.bulk_size}")
                _CLIENT = pymongo.MongoClient(
                    cfg.connection_string,
                    w=1,
                    maxPoolSize=200,
                    socketTimeoutMS=10000,
                    connectTimeoutMS=5000,
                )
                db = _CLIENT[cfg.database_name]
                self.coll = db[cfg.collection_name]

                # Ensure indexes exist before seeding
                self.coll.create_index([('dunsNumber', pymongo.ASCENDING)], unique=True, background=True)
                self.coll.create_index([('current.name', pymongo.ASCENDING)], background=True)
                self.coll.create_index([('current.address.state', pymongo.ASCENDING)], background=True)
                self.coll.create_index([('meta.last_modified', pymongo.DESCENDING)], background=True)
                print("[seed_core] Indexes ensured. Ready to seed.")

            except Exception as e:
                print(f"[seed_core] Initialization failed: {e}")
                if self.environment:
                    self.environment.runner.quit()
                raise

        self.client = _CLIENT
        self.coll   = self.client[cfg.database_name][cfg.collection_name]
        self.bulk_size = cfg.bulk_size

    # ------------------------------------------------------------------ #
    #  Data Generation                                                     #
    # ------------------------------------------------------------------ #
    def _generate_core_document(self):
        '''
        Generates a single entity_core document.

        Structure mirrors the agreed PoC data model:
          - current {}       → primary name + address (searchable)
          - former_names []  → populated by ops script on update
          - former_addresses [] → populated by ops script on update
          - financials {}    → latest snapshot + empty history array
          - legal {}         → structure type, registration state, SIC
          - employees {}     → headcount
          - assessment {}    → placeholder scores (updated by scoring job)
          - meta {}          → versioning, audit trail seed
        '''
        now   = datetime.utcnow()
        duns  = _('person.identifier', mask='#########')  # 9-digit numeric string
        doc_id = f"CORE-{duns}"

        return {
            '_id':        doc_id,
            'dunsNumber': duns,

            # Primary entity identity — name + address search target
            'current': {
                'name': _('finance.company'),
                'trade_style': None,
                'address': {
                    'line1':   _('address.street_name'),
                    'line2':   None,
                    'city':    _('address.city'),
                    'state':   _('address.state', abbr=True),
                    'zip':     _('address.zip_code'),
                    'country': 'US',
                },
                'phone':  _('person.phone_number'),
                'former_names':     [],   # populated on name change (ops script)
                'former_addresses': [],   # populated on address change (ops script)
            },

            # Legal / registration details
            'legal': {
                'structure':        random.choice(['Corporation', 'LLC', 'Partnership', 'Sole Proprietor']),
                'registration_state': _('address.state', abbr=True),
                'incorporated_date':  _('datetime.date', start=1990, end=2020).isoformat(),
                'sic_code':   str(_('numeric.integer_number', start=1000, end=9999)),
                'sic_desc':   _('finance.company_type'),
                'status':     random.choice(['active', 'inactive', 'suspended']),
            },

            # Employee headcount
            'employees': {
                'total':       _('numeric.integer_number', start=1, end=50000),
                'here':        _('numeric.integer_number', start=1, end=500),
                'reliability': random.choice(['Actual', 'Estimated', 'Modeled']),
            },

            # Financial snapshot — history populated by ops script
            'financials': {
                'currency': 'USD',
                'latest': {
                    'fiscal_year': datetime.utcnow().year - 1,
                    'total_revenue':  _('numeric.integer_number', start=100000, end=500000000),
                    'net_income':     _('numeric.integer_number', start=-5000000, end=100000000),
                    'total_assets':   _('numeric.integer_number', start=50000, end=1000000000),
                    'total_liabilities': _('numeric.integer_number', start=10000, end=500000000),
                    'cash':           _('numeric.integer_number', start=10000, end=50000000),
                    'as_of':          now,
                },
                'statements': [],  # historical statements appended by ops
            },

            # Assessment / scoring placeholder — updated by change-stream triggered scoring
            'assessment': {
                'credit_score':      _('numeric.integer_number', start=1, end=100),
                'failure_score':     _('numeric.integer_number', start=1, end=1000),
                'delinquency_score': _('numeric.integer_number', start=1, end=100),
                'payx_score':        None,   # populated when trade data is linked
                'last_scored':       now,
            },

            # Document metadata
            'meta': {
                'version':       1,
                'source_system': random.choice(['Batch', 'Research', 'API', 'Manual']),
                'quality_score': _('numeric.integer_number', start=50, end=100),
                'created_date':  _('datetime.datetime', start=2010, end=2022),
                'last_modified': now,
                'last_verified': now,
                'history':       [],  # action log, appended on every change
            },
        }

    # ------------------------------------------------------------------ #
    #  Task                                                                #
    # ------------------------------------------------------------------ #
    @task
    def bulk_upsert_core(self):
        '''
        Generates bulk_size documents and bulk-upserts into entity_core.
        Uses upsert=True so the script is safely re-runnable.
        '''
        task_name = "bulk_upsert_entity_core"
        tic = time.time()
        try:
            ops = [
                UpdateOne(
                    {'_id': doc['_id']},
                    {'$set': doc},
                    upsert=True
                )
                for doc in (self._generate_core_document() for _ in range(self.bulk_size))
            ]
            self.coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
            events.request.fire(
                request_type="mongodb",
                name=task_name,
                response_time=(time.time() - tic) * 1000,
                response_length=len(ops),
                exception=None,
            )
        except Exception as e:
            events.request.fire(
                request_type="mongodb",
                name=task_name,
                response_time=(time.time() - tic) * 1000,
                response_length=0,
                exception=e,
            )
