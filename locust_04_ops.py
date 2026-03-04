#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

MongoDB Atlas PoC — Script 4 of 4: OPS — UPDATES, AUDIT TRAIL & CHANGE DETECTION
========================================================================
Purpose:
    Simulates the real-time operational workload: targeted document
    updates from Research Agent, with full audit trail and change
    detection that mirrors the Atlas Change Stream publishing layer.

    This is the primary script for validating AP-5 (Change Detection
    → Downstream Publishing) and AP-6 (Cascading Score Updates).

Workflows:
    1. update_name_and_address
       Read → detect change → push former_name/former_address to history
       → update current → write audit record
       Validates: temporal data preservation, name search accuracy
                  after change, duns_audit log structure

    2. update_financials
       Blind write of new financials → append statement to history
       → write audit record
       Validates: high-throughput financial refresh (2M updates/day)

    3. update_assessment_scores
       Simulates the scoring job triggered by a Change Stream:
       read current scores → compute new scores → update assessment
       → record scoring event in duns_audit
       Validates: AP-6 cascading score update pattern

    4. simulate_bulk_file_processing
       Simulates a bulk file arriving with N records, only M of which
       contain actual changes. Finds the changed records, updates them,
       and writes change events to duns_audit (only for changed docs).
       Validates: AP-5 selective publishing — do NOT republish unchanged

Key design decisions (from Norm):
    - duns_audit is a SEPARATE collection (not embedded in core)
    - Audit captures: who changed it, what changed, old + new values
    - Only publish downstream if something ACTUALLY changed
    - Version counter incremented on every write (meta.version)
    - former_names / former_addresses arrays grow over time

Usage:
    With env vars (localized):
      export MONGODB_URI="mongodb+srv://..."
      export MONGODB_DATABASE="your_db"
      export MONGODB_COLLECTION="duns"
      export LOCUST_BULK_FILE_SIZE=50
      locust -f locust_04_ops.py --headless -u 10 -r 2 --run-time 10m

    Or with --host override (backward compatible):
      locust -f locust_04_ops.py \
             --host "mongodb+srv://<user>:<pass>@<cluster>|poc_demo|duns|50" \
             --headless -u 10 -r 2 --run-time 10m

Host format:  <connection_string>|<database>|<collection>|<bulk_file_size>
    bulk_file_size = number of records in simulated bulk file (task 4)
    Audit collection auto-derived as: <collection>_audit
'''

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import UpdateOne, InsertOne
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
_CLIENT      = None
_DUNS_SAMPLE = []   # In-memory sample of dunsNumbers for targeting updates


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    global _CLIENT
    if _CLIENT:
        print("Closing MongoDB connection...")
        _CLIENT.close()
        _CLIENT = None
        print("MongoDB connection closed.")


# ------------------------------------------------------------------ #
#  Helper                                                              #
# ------------------------------------------------------------------ #
def _fire(name, tic, response_length=0, exception=None):
    events.request.fire(
        request_type="mongodb",
        name=name,
        response_time=(time.time() - tic) * 1000,
        response_length=response_length,
        exception=exception,
    )


def _build_audit_record(duns, action, user, change_details):
    '''
    Standardized audit record for duns_audit collection.
    Mirrors the change stream event structure — captures who, what, when.
    '''
    return {
        'timestamp':      datetime.utcnow(),
        'dunsNumber':     duns,
        'action':         action,
        'user':           user,
        'publish_flag':   True,     # set to False if no actual change detected
        'change_details': change_details,
        'meta': {
            'source_script': 'locust_04_ops',
            'version_after': None,   # filled in after the update if needed
        }
    }


# ------------------------------------------------------------------ #
#  Locust User                                                         #
# ------------------------------------------------------------------ #
class OpsUser(User):
    '''
    Simulates a mix of research agent and batch processes performing
    real-time operational writes against entity_core.
    '''
    wait_time = between(1.0, 3.0)
    client      = None
    coll        = None
    audit_coll  = None
    bulk_file_size = 50

    def __init__(self, parent):
        global _CLIENT, _DUNS_SAMPLE
        super().__init__(parent)

        cfg = resolve_config(self.host)
        if not cfg or not cfg.connection_string:
            raise ValueError(
                "MongoDB connection required. Set MONGODB_URI (and optionally "
                "MONGODB_DATABASE, MONGODB_COLLECTION, LOCUST_BULK_FILE_SIZE) or use "
                "--host 'connection_string|db|collection|bulk_file_size'"
            )

        self.bulk_file_size = cfg.bulk_file_size

        if _CLIENT is None:
            try:
                print(f"[ops] Connecting... bulk_file_size={self.bulk_file_size}")
                _CLIENT = pymongo.MongoClient(
                    cfg.connection_string,
                    w=1,
                    maxPoolSize=100,
                    socketTimeoutMS=10000,
                    connectTimeoutMS=5000,
                )
                db = _CLIENT[cfg.database_name]
                self.coll       = db[cfg.collection_name]
                self.audit_coll = db[f"{cfg.collection_name}_audit"]

                # Ensure audit collection index
                self.audit_coll.create_index([('dunsNumber', pymongo.ASCENDING)], background=True)
                self.audit_coll.create_index([('timestamp',  pymongo.DESCENDING)], background=True)
                self.audit_coll.create_index([('publish_flag', pymongo.ASCENDING)], background=True)

                # Fetch a working sample of DUNS numbers
                print("[ops] Fetching dunsNumber sample...")
                pipeline = [
                    {'$sample': {'size': 2000}},
                    {'$project': {'dunsNumber': 1, '_id': 0}},
                ]
                _DUNS_SAMPLE = [doc['dunsNumber'] for doc in self.coll.aggregate(pipeline)]

                if not _DUNS_SAMPLE:
                    print("[ops] WARNING: No documents found. Run seed scripts first.")
                else:
                    print(f"[ops] Ready. Loaded {len(_DUNS_SAMPLE)} dunsNumbers for targeting.")

            except Exception as e:
                print(f"[ops] Initialization failed: {e}")
                if self.environment:
                    self.environment.runner.quit()
                raise

        self.client = _CLIENT
        self.coll            = self.client[cfg.database_name][cfg.collection_name]
        self.audit_coll      = self.client[cfg.database_name][f"{cfg.collection_name}_audit"]
        self.bulk_file_size  = cfg.bulk_file_size

    def _pick_duns(self):
        '''Returns a random DUNS number from the in-memory sample.'''
        if not _DUNS_SAMPLE:
            return None
        return random.choice(_DUNS_SAMPLE)

    # ---------------------------------------------------------------- #
    #  Task 1: Name + Address Update (AP-5 / Norm's primary use case)   #
    # ---------------------------------------------------------------- #
    @task(3)
    def update_name_and_address(self):
        '''
        Read-modify-write with full temporal history.
        Simulates research agent correcting a company name or address.

        On change:
          - Old name pushed to current.former_names[]
          - Old address pushed to current.former_addresses[]
          - New values written to current.name / current.address
          - Audit record written to duns_audit
          - publish_flag = True (change occurred → must publish)
        '''
        task_name = "update_name_and_address"
        tic = time.time()
        duns = self._pick_duns()
        if not duns:
            return

        try:
            # Step 1: Read current state (needed to preserve old values)
            original = self.coll.find_one(
                {'dunsNumber': duns},
                {'current': 1, 'meta.version': 1},
            )
            if not original or 'current' not in original:
                return

            now        = datetime.utcnow()
            old_name   = original['current'].get('name')
            old_address = original['current'].get('address', {})

            # Step 2: Generate new name + address
            new_name    = _('finance.company')
            new_address = {
                'line1':   _('address.street_name'),
                'line2':   None,
                'city':    _('address.city'),
                'state':   _('address.state', abbr=True),
                'zip':     _('address.zip_code'),
                'country': 'US',
            }

            # Step 3: Check if anything actually changed (AP-5 requirement)
            name_changed    = new_name != old_name
            address_changed = new_address != old_address
            anything_changed = name_changed or address_changed

            if not anything_changed:
                # Nothing changed — do NOT publish, but still record suppression
                self.audit_coll.insert_one({
                    'timestamp':    now,
                    'dunsNumber':   duns,
                    'action':       'NO_CHANGE_SUPPRESSED',
                    'publish_flag': False,
                    'user':         _('person.email'),
                })
                _fire(f"{task_name}[suppressed]", tic, response_length=0)
                return

            # Step 4: Build update payload
            push_payload = {
                'meta.history': {'action': 'NAME_ADDRESS_UPDATE', 'timestamp': now}
            }
            if name_changed:
                push_payload['current.former_names'] = {
                    'name':           old_name,
                    'effective_date': now,
                }
            if address_changed:
                push_payload['current.former_addresses'] = {
                    'address':        old_address,
                    'effective_date': now,
                }

            update_payload = {
                '$set': {
                    'current.name':    new_name,
                    'current.address': new_address,
                    'meta.last_modified': now,
                },
                '$inc':  {'meta.version': 1},
                '$push': push_payload,
            }

            # Step 5: Write main doc + audit record (two separate writes)
            self.coll.update_one({'dunsNumber': duns}, update_payload)
            self.audit_coll.insert_one(_build_audit_record(
                duns   = duns,
                action = 'NAME_ADDRESS_UPDATE',
                user   = _('person.email'),
                change_details = {
                    'name_changed':    name_changed,
                    'address_changed': address_changed,
                    'old_name':        old_name,
                    'new_name':        new_name,
                    'old_address':     old_address,
                    'new_address':     new_address,
                }
            ))

            _fire(task_name, tic, response_length=1)

        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  Task 2: Blind Financial Update                                    #
    # ---------------------------------------------------------------- #
    @task(4)
    def update_financials(self):
        '''
        High-frequency blind write of new financial data.
        Simulates the ~2M financial updates per day. No read required.
        Old financials appended to financials.statements[] for history.
        '''
        task_name = "update_financials"
        tic  = time.time()
        duns = self._pick_duns()
        if not duns:
            return

        try:
            now = datetime.utcnow()
            new_financials = {
                'fiscal_year':       now.year,
                'total_revenue':     _('numeric.integer_number', start=100000,    end=500000000),
                'net_income':        _('numeric.integer_number', start=-5000000,  end=100000000),
                'total_assets':      _('numeric.integer_number', start=50000,     end=1000000000),
                'total_liabilities': _('numeric.integer_number', start=10000,     end=500000000),
                'cash':              _('numeric.integer_number', start=10000,     end=50000000),
                'as_of':             now,
            }

            update_payload = {
                '$set':  {
                    'financials.latest':   new_financials,
                    'meta.last_modified':  now,
                },
                '$inc':  {'meta.version': 1},
                '$push': {
                    'financials.statements': {
                        '$each':  [new_financials],
                        '$slice': -24,   # keep last 24 periods (rolling window)
                    },
                    'meta.history': {'action': 'FINANCIAL_UPDATE', 'timestamp': now},
                },
            }

            audit_record = _build_audit_record(
                duns   = duns,
                action = 'FINANCIAL_UPDATE',
                user   = _('person.email', domains=['example-corp.com']),
                change_details = {'new_financials': new_financials}
            )

            self.coll.update_one({'dunsNumber': duns}, update_payload)
            self.audit_coll.insert_one(audit_record)

            _fire(task_name, tic, response_length=1)
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  Task 3: Assessment Score Update (AP-6 — Cascading Update)        #
    # ---------------------------------------------------------------- #
    @task(2)
    def update_assessment_scores(self):
        '''
        Simulates the scoring job that fires after a Change Stream event.
        In production this would be an Atlas Trigger. Here we run it
        directly to validate the scoring update pattern (AP-6).

        Reads current scores → computes new scores → writes back to core
        → records scoring event in duns_audit with publish_flag=True.
        '''
        task_name = "update_assessment_scores"
        tic  = time.time()
        duns = self._pick_duns()
        if not duns:
            return

        try:
            now = datetime.utcnow()

            # Simulate scores computed by the scoring engine
            new_scores = {
                'credit_score':      _('numeric.integer_number', start=1,   end=100),
                'failure_score':     _('numeric.integer_number', start=1,   end=1000),
                'delinquency_score': _('numeric.integer_number', start=1,   end=100),
                'payx_score':        _('numeric.integer_number', start=1,   end=100),
                'last_scored':       now,
            }

            update_payload = {
                '$set': {
                    'assessment':         new_scores,
                    'meta.last_modified': now,
                },
                '$inc':  {'meta.version': 1},
                '$push': {
                    'meta.history': {'action': 'SCORE_UPDATE', 'timestamp': now}
                },
            }

            audit_record = _build_audit_record(
                duns   = duns,
                action = 'SCORE_UPDATE',
                user   = 'scoring_engine@example-corp.com',
                change_details = {
                    'new_scores':    new_scores,
                    'triggered_by':  'change_stream',   # simulates AP-6 trigger source
                }
            )

            self.coll.update_one({'dunsNumber': duns}, update_payload)
            self.audit_coll.insert_one(audit_record)

            _fire(task_name, tic, response_length=1)
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  Task 4: Bulk File Processing with Selective Publishing (AP-5)     #
    # ---------------------------------------------------------------- #
    @task(1)
    def simulate_bulk_file_processing(self):
        '''
        AP-5 core validation: A bulk file arrives with N records.
        Only records with actual changes should be published.

        Workflow:
          1. Pull N random DUNS documents from the database
          2. For each, "generate" an incoming record
          3. Compare field-by-field to detect actual changes
          4. Batch-write only changed records back to core
          5. Write audit records ONLY for changed records (publish_flag=True)
          6. Write suppression records for unchanged (publish_flag=False)

        This directly validates Norm's requirement:
          "If I had 3 employees last time and I still have 3 employees,
           I don't need to republish that."
        '''
        task_name  = "bulk_file_processing"
        tic        = time.time()
        file_size  = self.bulk_file_size

        try:
            now = datetime.utcnow()

            # Step 1: Fetch N random documents to simulate incoming file records
            sample_docs = list(self.coll.aggregate([
                {'$sample': {'size': file_size}},
                {'$project': {'dunsNumber': 1, 'current': 1, 'meta.version': 1}},
            ]))

            if not sample_docs:
                return

            # Step 2: Simulate incoming file — ~30% of records have a name change
            CHANGE_RATE = 0.30
            core_ops    = []
            audit_ops   = []
            changed     = 0
            suppressed  = 0

            for doc in sample_docs:
                duns    = doc['dunsNumber']
                current = doc.get('current', {})
                old_name = current.get('name', '')

                # Simulate incoming record — sometimes a name change, sometimes not
                if random.random() < CHANGE_RATE:
                    new_name = _('finance.company')
                else:
                    new_name = old_name   # no change

                if new_name != old_name:
                    # Changed — update + publish
                    changed += 1
                    core_ops.append(UpdateOne(
                        {'dunsNumber': duns},
                        {
                            '$set': {
                                'current.name':    new_name,
                                'meta.last_modified': now,
                            },
                            '$inc':  {'meta.version': 1},
                            '$push': {
                                'current.former_names': {
                                    'name': old_name, 'effective_date': now
                                },
                                'meta.history': {'action': 'BULK_FILE_NAME_CHANGE', 'timestamp': now},
                            },
                        }
                    ))
                    audit_ops.append(InsertOne({
                        'timestamp':    now,
                        'dunsNumber':   duns,
                        'action':       'BULK_FILE_NAME_CHANGE',
                        'publish_flag': True,   # CHANGED → publish downstream
                        'user':         'bulk_file_processor@example-corp.com',
                        'change_details': {
                            'old_name': old_name,
                            'new_name': new_name,
                        }
                    }))
                else:
                    # Unchanged — do NOT publish
                    suppressed += 1
                    audit_ops.append(InsertOne({
                        'timestamp':    now,
                        'dunsNumber':   duns,
                        'action':       'BULK_FILE_NO_CHANGE',
                        'publish_flag': False,  # UNCHANGED → suppress publishing
                        'user':         'bulk_file_processor@example-corp.com',
                        'change_details': {}
                    }))

            # Step 3: Execute bulk writes (core changes only, all audit records)
            if core_ops:
                self.coll.bulk_write(core_ops, ordered=False)
            if audit_ops:
                self.audit_coll.bulk_write(audit_ops, ordered=False)

            # Report changed vs suppressed in the task name for Locust UI visibility
            _fire(
                f"{task_name}[changed={changed},suppressed={suppressed}]",
                tic,
                response_length=len(sample_docs),
            )

        except Exception as e:
            _fire(task_name, tic, exception=e)
