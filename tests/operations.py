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

from mockupdb import OpReply, REPLY_FLAGS
from pymongo import ReadPreference

Operation = namedtuple(
    'operation',
    ['name', 'function', 'reply', 'op_type', 'wire_version', 'not_master'])

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
