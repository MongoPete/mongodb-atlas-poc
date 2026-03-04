#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

MongoDB Atlas PoC — Script 3 of 4: ATLAS SEARCH (ACCESS PATTERNS 1, 3, 4)
=================================================================
Purpose:
    Validates all name & address search access patterns identified
    in the March 2 session. Run AFTER the seed scripts have loaded data.

Access patterns covered:
    AP-1  Core name & address → DUNS candidate list
          "Go find all the ABCs" — fuzzy, not literal
    AP-2  DUNS number → full document retrieval (point lookup on core)
    AP-3  Unmatched trade account → UUID candidate list
    AP-4  Role player name & address → public records candidate list
          (matched AND unmatched; cross collection)

Address relaxation logic (Norm's requirement):
    Always return something. If Street+City+State finds nothing,
    relax to City+State, then State-only. Return top-10 with scores.

Pre-requisites:
    Atlas Search indexes must exist. Create these in the Atlas UI
    or via the Data API before running this script:

    entity_core  → index name: "core_search"
                   { mappings: { fields: {
                       "current.name":         fuzzy text,
                       "current.address.city":  text,
                       "current.address.state": text,
                       "current.address.line1": text
                   }}}

    entity_trade → index name: "trade_search"
                   { mappings: { fields: {
                       "account.name":         fuzzy text,
                       "account.address.city":  text,
                       "account.address.state": text,
                       "matched":               boolean
                   }}}

    suits / liens / judgments / uccs / bankruptcies
                 → index name: "public_records_search"
                   { mappings: { fields: {
                       "role_players.names.name":       fuzzy text,
                       "role_players.addresses.city":   text,
                       "role_players.addresses.state":  text,
                       "matched":                       boolean
                   }}}

Usage:
    With env vars (localized):
      export MONGODB_URI="mongodb+srv://..."
      export MONGODB_DATABASE="your_db"
      export LOCUST_CANDIDATE_LIMIT=10
      locust -f locust_03_search.py --headless -u 20 -r 5 --run-time 10m

    Or with --host override (backward compatible):
      locust -f locust_03_search.py \
             --host "mongodb+srv://<user>:<pass>@<cluster>|poc_demo|10" \
             --headless -u 20 -r 5 --run-time 10m

Host format:  <connection_string>|<database>|<candidate_limit>
'''

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from locust import User, events, task, between

from locust_db_config import resolve_config
import time
import random
from mimesis import Field
from mimesis.locales import Locale

# --- Mimesis Global ---
_ = Field(locale=Locale.EN)

# --- Company name suffix variants (mimics Norm's "find all ABCs" requirement) ---
NAME_SUFFIXES = ['Inc', 'Inc.', 'LLC', 'Co', 'Co.', 'Corp', 'Corporation', 'Company', '']

# --- Public records collections to search across (default; overridable via config) ---
PUBLIC_RECORDS_COLLECTIONS = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']

# --- Global State ---
_CLIENT   = None
_DB       = None
_CAND_LIM = 10


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    global _CLIENT
    if _CLIENT:
        print("Closing MongoDB connection...")
        _CLIENT.close()
        _CLIENT = None
        print("MongoDB connection closed.")


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #
def _fire(name, tic, response_length=0, exception=None):
    events.request.fire(
        request_type="mongodb",
        name=name,
        response_time=(time.time() - tic) * 1000,
        response_length=response_length,
        exception=exception,
    )


def _fuzzy_name_variants(base_name):
    '''
    Returns the base company name with a random suffix stripped or added.
    Simulates the "find ABC Co Inc, ABC Co, ABC Inc, ABC" requirement.
    '''
    words = base_name.split()
    # Strip known suffixes if present
    if words and words[-1] in NAME_SUFFIXES:
        words = words[:-1]
    # Optionally re-add a different suffix
    suffix = random.choice(NAME_SUFFIXES)
    return (' '.join(words) + (' ' + suffix if suffix else '')).strip()


# ------------------------------------------------------------------ #
#  Atlas Search Pipeline Builders                                      #
# ------------------------------------------------------------------ #
def _build_core_search_pipeline(name, city, state, street=None, candidate_limit=10):
    '''
    Builds a $search pipeline for AP-1: Core name + address → DUNS.

    Uses compound query with:
      - must:   name fuzzy match (non-negotiable)
      - should: address fields boosted for relevance scoring
      - minimumShouldMatch: 0 (address is optional but boosts score)
    '''
    should_clauses = [
        {'text': {'query': city,  'path': 'current.address.city',  'score': {'boost': {'value': 2}}}},
        {'text': {'query': state, 'path': 'current.address.state', 'score': {'boost': {'value': 1}}}},
    ]
    if street:
        should_clauses.append(
            {'text': {'query': street, 'path': 'current.address.line1', 'score': {'boost': {'value': 3}}}}
        )

    return [
        {'$search': {
            'index': 'core_search',
            'compound': {
                'must': [
                    {'text': {
                        'query': name,
                        'path':  'current.name',
                        'fuzzy': {'maxEdits': 1, 'prefixLength': 2},
                    }}
                ],
                'should': should_clauses,
            }
        }},
        {'$limit': candidate_limit},
        {'$project': {
            'dunsNumber':     1,
            'current.name':   1,
            'current.address': 1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


def _build_trade_search_pipeline(name, city, state, candidate_limit=10):
    '''AP-3: Unmatched trade account name + address → UUID candidate list.'''
    return [
        {'$search': {
            'index': 'trade_search',
            'compound': {
                'must': [
                    {'text': {
                        'query': name,
                        'path':  'account.name',
                        'fuzzy': {'maxEdits': 1, 'prefixLength': 2},
                    }},
                    {'equals': {'value': False, 'path': 'matched'}},  # unmatched only
                ],
                'should': [
                    {'text': {'query': city,  'path': 'account.address.city',  'score': {'boost': {'value': 2}}}},
                    {'text': {'query': state, 'path': 'account.address.state', 'score': {'boost': {'value': 1}}}},
                ],
            }
        }},
        {'$limit': candidate_limit},
        {'$project': {
            'uuid':          1,
            'account.name':  1,
            'account.address': 1,
            'matched':       1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


def _build_role_player_search_pipeline(name, city, state, candidate_limit=10):
    '''
    AP-4: Role player name + address search across a public records collection.
    Searches inside the role_players[] array (nested fields).
    Returns matched/unmatched candidates so research agent can decide.
    '''
    return [
        {'$search': {
            'index': 'public_records_search',
            'compound': {
                'must': [
                    {'text': {
                        'query': name,
                        'path':  'role_players.names.name',
                        'fuzzy': {'maxEdits': 1, 'prefixLength': 2},
                    }}
                ],
                'should': [
                    {'text': {'query': city,  'path': 'role_players.addresses.city',  'score': {'boost': {'value': 2}}}},
                    {'text': {'query': state, 'path': 'role_players.addresses.state', 'score': {'boost': {'value': 1}}}},
                ],
            }
        }},
        {'$limit': candidate_limit},
        {'$project': {
            'filing_id':    1,
            'filing_type':  1,
            'matched':      1,
            'duns_numbers': 1,
            'role_players.role_type':  1,
            'role_players.polarity':   1,
            'role_players.duns_number': 1,
            'role_players.matched':    1,
            'role_players.names':      1,
            'score': {'$meta': 'searchScore'},
        }},
    ]


# ------------------------------------------------------------------ #
#  Address Relaxation Logic                                            #
# ------------------------------------------------------------------ #
def _relaxed_core_search(coll, name, street, city, state, candidate_limit):
    '''
    AP-1 with address relaxation (Norm's "always return something" rule).

    Attempts progressively looser searches:
      1. Name + Street + City + State
      2. Name + City + State
      3. Name + State only
      4. Name only (national)

    Returns (results, relaxation_level_used)
    '''
    attempts = [
        (street, city, state, 'street_city_state'),
        (None,   city, state, 'city_state'),
        (None,   None, state, 'state_only'),
        (None,   None, None,  'name_only'),
    ]

    for street_v, city_v, state_v, level in attempts:
        pipeline = _build_core_search_pipeline(
            name=name,
            city=city_v or '',
            state=state_v or '',
            street=street_v,
            candidate_limit=candidate_limit,
        )
        results = list(coll.aggregate(pipeline))
        if results:
            return results, level

    return [], 'exhausted'


# ------------------------------------------------------------------ #
#  Locust User                                                         #
# ------------------------------------------------------------------ #
class SearchUser(User):
    '''
    Simulates concurrent research agent and application searches
    running all four search access patterns simultaneously.

    Task weights reflect expected real-world distribution:
      - AP-1 core search is most frequent
      - AP-4 public records role player search is next
      - AP-2 DUNS retrieval (point lookup) is fast and frequent
      - AP-3 unmatched trade search is less frequent
    '''
    wait_time = between(0.5, 2.0)
    client    = None
    db        = None
    cand_lim  = 10
    core_collection = 'duns'
    trade_collection = 'entity_trade'
    public_records_collections = None

    def __init__(self, parent):
        global _CLIENT, _DB, _CAND_LIM
        super().__init__(parent)

        cfg = resolve_config(self.host)
        if not cfg or not cfg.connection_string:
            raise ValueError(
                "MongoDB connection required. Set MONGODB_URI (and optionally "
                "MONGODB_DATABASE, LOCUST_CANDIDATE_LIMIT) or use "
                "--host 'connection_string|db|candidate_limit'"
            )

        if _CLIENT is None:
            try:
                _CAND_LIM = cfg.candidate_limit
                print(f"[search] Connecting... candidate_limit={_CAND_LIM}")
                _CLIENT = pymongo.MongoClient(
                    cfg.connection_string,
                    w=1,
                    maxPoolSize=200,
                    socketTimeoutMS=15000,  # search can take longer
                    connectTimeoutMS=5000,
                )
                _DB = _CLIENT[cfg.database_name]
                print("[search] Connected. Ready to run search tasks.")

            except Exception as e:
                print(f"[search] Initialization failed: {e}")
                if self.environment:
                    self.environment.runner.quit()
                raise

        self.client   = _CLIENT
        self.db       = _DB
        self.cand_lim = cfg.candidate_limit
        self.core_collection = cfg.core_collection
        self.trade_collection = cfg.trade_collection
        self.public_records_collections = cfg.public_records_collections

    # ---------------------------------------------------------------- #
    #  AP-1: Core Name + Address → DUNS Candidate List                  #
    # ---------------------------------------------------------------- #
    @task(5)
    def ap1_core_name_address_search(self):
        '''
        AP-1: Most common search. Simulates research agent looking up
        a company by name + address to find its DUNS number.
        Applies address relaxation — always returns something.
        '''
        task_name = "ap1_core_name_addr_search"
        tic = time.time()
        try:
            name   = _fuzzy_name_variants(_('finance.company'))
            street = _('address.street_name')
            city   = _('address.city')
            state  = _('address.state', abbr=True)

            coll = self.db[self.core_collection]
            results, level = _relaxed_core_search(coll, name, street, city, state, self.cand_lim)

            _fire(f"{task_name}[{level}]", tic, response_length=len(results))
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  AP-2: DUNS Number → Full Document Retrieval                       #
    # ---------------------------------------------------------------- #
    @task(4)
    def ap2_duns_point_lookup(self):
        '''
        AP-2: Direct key access by DUNS number. Fast — hits the unique
        index on dunsNumber. Represents the second half of the research agent
        workflow: candidate selected → go get the full record.
        '''
        task_name = "ap2_duns_point_lookup"
        tic = time.time()
        try:
            # Generate a synthetic DUNS number to query — in a real test
            # this would come from AP-1 results. We use a random sample
            # pipeline to simulate pulling a real DUNS from the database.
            coll = self.db[self.core_collection]
            sample = list(coll.aggregate([
                {'$sample': {'size': 1}},
                {'$project': {'dunsNumber': 1}},
            ]))

            if not sample:
                _fire(task_name, tic, exception=Exception("No documents found"))
                return

            duns = sample[0]['dunsNumber']
            doc  = coll.find_one({'dunsNumber': duns})

            _fire(task_name, tic, response_length=1 if doc else 0)
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  AP-3: Unmatched Trade Account Search                              #
    # ---------------------------------------------------------------- #
    @task(2)
    def ap3_unmatched_trade_search(self):
        '''
        AP-3: research agent searching for trade accounts that haven't
        been matched to a DUNS number yet. Fuzzy name + address.
        '''
        task_name = "ap3_unmatched_trade_search"
        tic = time.time()
        try:
            name  = _fuzzy_name_variants(_('finance.company'))
            city  = _('address.city')
            state = _('address.state', abbr=True)

            coll     = self.db[self.trade_collection]
            pipeline = _build_trade_search_pipeline(name, city, state, self.cand_lim)
            results  = list(coll.aggregate(pipeline))

            _fire(task_name, tic, response_length=len(results))
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  AP-4: Public Records Role Player Search                           #
    # ---------------------------------------------------------------- #
    @task(3)
    def ap4_public_records_role_player_search(self):
        '''
        AP-4: Searches role player names + addresses across a randomly
        selected public records collection (suits, liens, judgments, etc.)

        Simulates research agent looking for an entity across public
        records to associate it with a DUNS number.
        Applies address relaxation — always returns something.
        '''
        task_name = "ap4_public_records_role_player_search"
        tic = time.time()
        try:
            name       = _fuzzy_name_variants(_('finance.company'))
            city       = _('address.city')
            state      = _('address.state', abbr=True)
            filing_col = random.choice(self.public_records_collections)

            coll = self.db[filing_col]

            # Address relaxation for public records too
            relaxation_attempts = [
                (city, state, 'city_state'),
                (None, state, 'state_only'),
                (None, None,  'name_only'),
            ]

            results = []
            level   = 'exhausted'
            for city_v, state_v, lv in relaxation_attempts:
                pipeline = _build_role_player_search_pipeline(
                    name=name,
                    city=city_v or '',
                    state=state_v or '',
                    candidate_limit=self.cand_lim,
                )
                results = list(coll.aggregate(pipeline))
                if results:
                    level = lv
                    break

            _fire(f"{task_name}[{filing_col}][{level}]", tic, response_length=len(results))
        except Exception as e:
            _fire(task_name, tic, exception=e)

    # ---------------------------------------------------------------- #
    #  AP-4b: Unmatched-Only Public Records Filter                       #
    # ---------------------------------------------------------------- #
    @task(1)
    def ap4b_unmatched_public_records_filter(self):
        '''
        AP-4b: Queries the matched=false index to find all unmatched
        filings across a collection. Simulates the "give me all
        unmatched public records" batch job Norm described.
        '''
        task_name = "ap4b_unmatched_public_records_filter"
        tic = time.time()
        try:
            filing_col = random.choice(self.public_records_collections)
            coll       = self.db[filing_col]

            # Efficient index scan on matched=False
            results = list(
                coll.find(
                    {'matched': False},
                    {'filing_id': 1, 'filing_type': 1, 'role_players.names': 1},
                ).limit(self.cand_lim)
            )

            _fire(f"{task_name}[{filing_col}]", tic, response_length=len(results))
        except Exception as e:
            _fire(task_name, tic, exception=e)
