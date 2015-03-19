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
from pymongo import ReadPreference

# For testing the SlaveOkay bit setting, see:
# https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#passing-read-preference-to-mongos
Operation = namedtuple('operation', ['name', 'function', 'reply', 'op_type'])

operations = [
    Operation(
        'find_one',
        lambda client: client.db.collection.find_one(),
        reply={},
        op_type='may-use-secondary'),
    Operation(
        'count',
        lambda client: client.db.collection.count(),
        reply={'n': 1},
        op_type='may-use-secondary'),
    Operation(
        'aggregate',
        lambda client: client.db.collection.aggregate([]),
        reply={'result': []},
        op_type='may-use-secondary'),
    Operation(
        'command',
        lambda client: client.db.command('foo'),
        reply={'ok': 1},
        op_type='must-use-primary'),  # Ignores client's read preference.
    Operation(
        'secondary command',
        lambda client:
            client.db.command('foo', read_preference=ReadPreference.SECONDARY),
        reply={'ok': 1},
        op_type='always-use-secondary'),
    Operation(
        'collection_names',
        lambda client: client.db.collection_names(),
        reply=[],
        op_type='must-use-primary'),
]
