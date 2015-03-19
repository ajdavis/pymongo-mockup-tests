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

Operation = namedtuple('operation',
                       ['name', 'function', 'reply', 'op_type', 'not_master'])

operations = [
    Operation(
        'find_one',
        lambda client: client.db.collection.find_one(),
        reply={},
        op_type='may-use-secondary',
        not_master=OpReply({'$err': 'not master'},
                           flags=REPLY_FLAGS['QueryFailure'])),
    Operation(
        'count',
        lambda client: client.db.collection.count(),
        reply={'n': 1},
        op_type='may-use-secondary',
        not_master=OpReply(ok=0, errmsg='not master')),
    Operation(
        'aggregate',
        lambda client: client.db.collection.aggregate([]),
        reply={'result': []},
        op_type='may-use-secondary',
        not_master=OpReply(ok=0, errmsg='not master')),
    Operation(
        'command',
        lambda client: client.db.command('foo'),
        reply={'ok': 1},
        op_type='must-use-primary',  # Ignores client's read preference.
        not_master=OpReply(ok=0, errmsg='not master')),
    Operation(
        'secondary command',
        lambda client:
            client.db.command('foo', read_preference=ReadPreference.SECONDARY),
        reply={'ok': 1},
        op_type='always-use-secondary',
        not_master=OpReply(ok=0, errmsg='node is recovering')),
    Operation(
        'collection_names',
        lambda client: client.db.collection_names(),
        reply=[],
        op_type='must-use-primary',
        not_master=OpReply({'$err': 'not master'},
                           flags=REPLY_FLAGS['QueryFailure'])),
]
