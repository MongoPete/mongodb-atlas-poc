#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: MODULE NEEDS TO BE COMPATIBLE WITH PYPY3!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

Shared configuration for MongoDB Atlas PoC Locust scripts.
Resolves MongoDB connection, database, and collection from environment
variables and/or Locust --host pipe-delimited override.
'''

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class LocustDBConfig(object):
    '''
    Resolved config: connection_string, database_name, collection_name,
    and script-specific numeric params (bulk_size, candidate_limit, bulk_file_size).
    Also optional collection names for search script (core, trade, public records).
    '''
    __slots__ = (
        'connection_string',
        'database_name',
        'collection_name',
        'bulk_size',
        'candidate_limit',
        'bulk_file_size',
        'core_collection',
        'trade_collection',
        'public_records_collections',
    )

    def __init__(
        self,
        connection_string,
        database_name,
        collection_name=None,
        bulk_size=500,
        candidate_limit=10,
        bulk_file_size=50,
        core_collection='duns',
        trade_collection='entity_trade',
        public_records_collections=None,
    ):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.bulk_size = int(bulk_size)
        self.candidate_limit = int(candidate_limit)
        self.bulk_file_size = int(bulk_file_size)
        self.core_collection = core_collection
        self.trade_collection = trade_collection
        if public_records_collections is None:
            public_records_collections = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']
        self.public_records_collections = public_records_collections


def _env(key, default=None):
    return os.environ.get(key, default or '').strip() or default


def _env_int(key, default):
    v = _env(key)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def resolve_config(host=None):
    '''
    Build config from environment variables; if host is non-empty and
    contains '|', parse pipe-delimited override.

    Env vars:
      MONGODB_URI or MONGO_URI           -> connection_string
      MONGODB_DATABASE or MONGO_DB       -> database_name
      MONGODB_COLLECTION or MONGO_COLLECTION -> collection_name
      LOCUST_BULK_SIZE                   -> bulk_size (default 500)
      LOCUST_CANDIDATE_LIMIT             -> candidate_limit (default 10)
      LOCUST_BULK_FILE_SIZE              -> bulk_file_size (default 50)
      MONGODB_CORE_COLLECTION            -> core_collection (default duns)
      MONGODB_TRADE_COLLECTION           -> trade_collection (default entity_trade)
      MONGODB_PUBLIC_RECORDS_COLLECTIONS -> comma-separated list (default suits,liens,...)

    Host override:
      - 4 parts: connection_string|database|collection|numeric
        -> overrides connection, db, collection; numeric sets both bulk_size and bulk_file_size
      - 3 parts: connection_string|database|candidate_limit
        -> overrides connection, db, candidate_limit (search script)
    '''
    uri = _env('MONGODB_URI') or _env('MONGO_URI')
    db = _env('MONGODB_DATABASE') or _env('MONGO_DB') or 'poc_demo'
    coll = _env('MONGODB_COLLECTION') or _env('MONGO_COLLECTION') or 'duns'
    bulk_size = _env_int('LOCUST_BULK_SIZE', 500)
    candidate_limit = _env_int('LOCUST_CANDIDATE_LIMIT', 10)
    bulk_file_size = _env_int('LOCUST_BULK_FILE_SIZE', 50)
    core_coll = _env('MONGODB_CORE_COLLECTION') or 'duns'
    trade_coll = _env('MONGODB_TRADE_COLLECTION') or 'entity_trade'
    pr_str = _env('MONGODB_PUBLIC_RECORDS_COLLECTIONS')
    if pr_str:
        public_records_collections = [s.strip() for s in pr_str.split(',') if s.strip()]
    else:
        public_records_collections = ['suits', 'liens', 'judgments', 'uccs', 'bankruptcies']

    if host and '|' in host:
        parts = host.split('|')
        if len(parts) == 4:
            uri = parts[0].strip()
            db = parts[1].strip()
            coll = parts[2].strip()
            try:
                num = int(parts[3].strip())
                bulk_size = num
                bulk_file_size = num
            except ValueError:
                pass
        elif len(parts) == 3:
            uri = parts[0].strip()
            db = parts[1].strip()
            try:
                candidate_limit = int(parts[2].strip())
            except ValueError:
                pass

    if not uri:
        return None

    return LocustDBConfig(
        connection_string=uri,
        database_name=db,
        collection_name=coll,
        bulk_size=bulk_size,
        candidate_limit=candidate_limit,
        bulk_file_size=bulk_file_size,
        core_collection=core_coll,
        trade_collection=trade_coll,
        public_records_collections=public_records_collections,
    )
