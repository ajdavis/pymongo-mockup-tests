# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"),;
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple

from mockupdb import *
from mockupdb import OpUpdate
from pymongo import ReadPreference

__all__ = ['operations', 'upgrades']


Operation = namedtuple(
    'operation',
    ['name', 'function', 'reply', 'op_type', 'wire_version', 'not_master'])
"""Client operations on MongoDB.

Each has a human-readable name, a function that actually executes a test, and
a type that maps to one of the types in the Server Selection Spec:
'may-use-secondary', 'must-use-primary', etc.

The special type 'always-use-secondary' applies to an operation with an explicit
read mode, like the operation "command('c', read_preference=SECONDARY)".

The wire version is the version in which the operation was introduced, and
the not-master response is how a secondary responds to a must-use-primary op,
or how a recovering member responds to a may-use-secondary op.

Example uses:

We can use "find_one" to validate that the SlaveOk bit is set when querying a
standalone, even with mode PRIMARY, but that it isn't set when sent to a mongos
with mode PRIMARY. Or it can validate that "$readPreference" is included in
mongos queries except with mode PRIMARY or SECONDARY_PREFERRED (PYTHON-865).

We can use "options_old" and "options_new" to test that the driver queries an
old server's system.namespaces collection, but uses the listCollections command
on a new server (PYTHON-857).

"secondary command" is good to test that the client can direct reads to
secondaries in a replica set, or select a mongos for secondary reads in a
sharded cluster (PYTHON-868).
"""

not_master_reply_to_query = OpReply(
    {'$err': 'not master'},
    flags=REPLY_FLAGS['QueryFailure'])

not_master_reply_to_command = OpReply(ok=0, errmsg='not master')

operations = [
    Operation(
        'find_one',
        lambda client: client.db.collection.find_one(),
        reply={},
        op_type='may-use-secondary',
        wire_version=0,
        not_master=not_master_reply_to_query),
    Operation(
        'count',
        lambda client: client.db.collection.count(),
        reply={'n': 1},
        op_type='may-use-secondary',
        wire_version=0,
        not_master=not_master_reply_to_command),
    Operation(
        'aggregate',
        lambda client: client.db.collection.aggregate([]),
        reply={'result': []},
        op_type='may-use-secondary',
        wire_version=0,
        not_master=not_master_reply_to_command),
    Operation(
        'mapreduce',
        lambda client: client.db.collection.map_reduce(
            'function() {}', 'function() {}', 'out_collection'),
        reply={'result': {'db': 'db', 'collection': 'out_collection'}},
        op_type='must-use-primary',
        wire_version=0,
        not_master=not_master_reply_to_command),
    Operation(
        'inline_mapreduce',
        lambda client: client.db.collection.inline_map_reduce(
            'function() {}', 'function() {}', {'out': {'inline': 1}}),
        reply={'results': []},
        op_type='may-use-secondary',
        wire_version=0,
        not_master=not_master_reply_to_command),
    Operation(
        'options_old',
        lambda client: client.db.collection.options(),
        reply={'results': []},
        op_type='must-use-primary',
        wire_version=0,
        not_master=not_master_reply_to_query),
    Operation(
        'options_new',
        lambda client: client.db.collection.options(),
        reply={'cursor': {'id': 0, 'firstBatch': []}},
        op_type='must-use-primary',
        wire_version=3,
        not_master=not_master_reply_to_command),
    Operation(
        'command',
        lambda client: client.db.command('foo'),
        reply={'ok': 1},
        op_type='must-use-primary',  # Ignores client's read preference.
        wire_version=0,
        not_master=not_master_reply_to_command),
    Operation(
        'secondary command',
        lambda client:
            client.db.command('foo', read_preference=ReadPreference.SECONDARY),
        reply={'ok': 1},
        op_type='always-use-secondary',
        wire_version=0,
        not_master=OpReply(ok=0, errmsg='node is recovering')),
    Operation(
        'collection_names',
        lambda client: client.db.collection_names(),
        reply=[],
        op_type='must-use-primary',
        wire_version=0,
        not_master=not_master_reply_to_query),
    Operation(
        'listCollections',
        lambda client: client.db.collection_names(),
        reply={'cursor': {'id': 0, 'firstBatch': []}},
        op_type='must-use-primary',
        wire_version=3,
        not_master=not_master_reply_to_command),
    Operation(
        'system_indexes',
        lambda client: client.db.collection.index_information(),
        reply=[],
        op_type='must-use-primary',
        wire_version=0,
        not_master=not_master_reply_to_query),
    Operation(
        'listIndexes',
        lambda client: client.db.collection.index_information(),
        reply={'cursor': {'id': 0, 'firstBatch': []}},
        op_type='must-use-primary',
        wire_version=3,
        not_master=not_master_reply_to_command),
]


_ops_by_name = dict([(op.name, op) for op in operations])

Upgrade = namedtuple('Upgrade',
                     ['name', 'function', 'old', 'new', 'wire_version'])

upgrades = [
    Upgrade('insert_one',
            lambda client: client.db.collection.insert_one({}),
            old=OpInsert(),
            new=Command('insert', 'collection'),
            wire_version=2),
    Upgrade('replace_one',
            lambda client: client.db.collection.replace_one({}, {}),
            old=OpUpdate(),
            new=Command('update', 'collection'),
            wire_version=2),
    Upgrade('delete_one',
            lambda client: client.db.collection.delete_one({}),
            old=OpDelete(),
            new=Command('delete', 'collection'),
            wire_version=2),
    Upgrade('aggregation cursor',
            lambda client: client.db.collection.aggregate([]),
            old=Command('aggregate', 'collection', cursor=absent),
            new=Command('aggregate', 'collection', cursor={}),
            wire_version=1),
    Upgrade('index_information',
            lambda client: client.db.collection.index_information(),
            old=OpQuery(namespace='db.system.indexes'),
            new=Command('listIndexes', 'collection', namespace='db'),
            wire_version=3),
    Upgrade('collection_names',
            lambda client: client.db.collection_names(),
            old=OpQuery(namespace='db.system.namespaces'),
            new=Command('listCollections', namespace='db'),
            wire_version=3),
    Upgrade('options',
            lambda client: client.db.collection.options(),
            old=OpQuery(namespace='db.system.namespaces'),
            new=Command('listCollections', 'collection', namespace='db'),
            wire_version=3),
]
