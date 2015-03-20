# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

import itertools

from mockupdb import MockupDB, going, QUERY_FLAGS
from pymongo import MongoClient, ReadPreference
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name,
                                      _MONGOS_MODES)

from tests import unittest
from tests.operations import operations


class TestMongosCommandReadMode(unittest.TestCase):
    def test_aggregate(self):
        server = MockupDB()
        server.autoresponds('ismaster', ismaster=True, msg='isdbgrid')
        self.addCleanup(server.stop)
        server.run()

        collection = MongoClient(server.uri).test.collection
        with going(collection.aggregate, []):
            command = server.receives(aggregate='collection', pipeline=[])
            self.assertNotIn('$readPreference', command)
            command.ok(result=[{}])

        secondary_collection = collection.with_options(
            read_preference=ReadPreference.SECONDARY)

        with going(secondary_collection.aggregate, []):
            command = server.receives(aggregate='collection', pipeline=[])
            command.ok(result=[{}])
            self.assertEqual({'mode': 'secondary'},
                             command.doc.get('$readPreference'))


def create_mongos_read_mode_test(mode, operation):
    def test(self):
        server = MockupDB()
        self.addCleanup(server.stop)
        server.run()
        server.autoresponds('ismaster', ismaster=True, msg='isdbgrid',
                            maxWireVersion=operation.wire_version)

        if operation.op_type in ('always-use-secondary', 'may-use-secondary'):
            slave_ok = mode != 'primary'
        elif operation.op_type == 'must-use-primary':
            slave_ok = False
        else:
            assert False, 'unrecognized op_type %r' % operation.op_type

        pref = make_read_preference(read_pref_mode_from_name(mode),
                                    tag_sets=None)

        client = MongoClient(server.uri, read_preference=pref)
        with going(operation.function, client):
            request = server.receive()
            request.reply(operation.reply)

        if slave_ok:
            self.assertTrue(request.flags & QUERY_FLAGS['SlaveOkay'],
                            'SlaveOkay not set')
        else:
            self.assertFalse(request.flags & QUERY_FLAGS['SlaveOkay'],
                             'SlaveOkay set')

        if mode in ('primary', 'secondary'):
            self.assertNotIn('$readPreference', request)
        else:
            self.assertEqual(pref.document, request.doc.get('$readPreference'))

    return test


def generate_mongos_read_mode_tests():
    matrix = itertools.product(_MONGOS_MODES, operations)

    for entry in matrix:
        mode, operation = entry
        if mode == 'primary' and operation.op_type == 'always-use-secondary':
            # Skip something like command('foo', read_preference=SECONDARY).
            continue
        test = create_mongos_read_mode_test(mode, operation)
        test_name = 'test_%s_with_mode_%s' % (
            operation.name.replace(' ', '_'), mode)
        test.__name__ = test_name
        setattr(TestMongosCommandReadMode, test_name, test)


generate_mongos_read_mode_tests()


if __name__ == '__main__':
    unittest.main()
