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

"""Test PyMongo's SlaveOkay with:

  - A direct connection to a standalone
  - A direct connection to a slave
  - A direct connection to a mongos
  - A load-balanced connection to two mongoses
"""

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from pymongo import MongoClient
from pymongo.topology_description import TOPOLOGY_TYPE

from mockupdb import MockupDB, OpQuery, going, QUERY_FLAGS

from tests import unittest


def topology_type_name(client):
    topology_type = client._topology._description.topology_type
    return TOPOLOGY_TYPE._fields[topology_type]


class TestSlaveOkaySingle(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB()
        self.server.run()
        self.addCleanup(self.server.stop)

    def _test_slave_okay(self, expect_slave_okay):
        client = MongoClient(self.server.uri)
        with going(client.db.collection.find_one):
            query = self.server.gets(OpQuery)
            query.reply({'one': 'doc'})

        self.assertEqual(topology_type_name(client), 'Single')
        if expect_slave_okay:
            self.assertTrue(query.flags & QUERY_FLAGS['SlaveOkay'],
                            'SlaveOkay not set with topology type Single')
        else:
            self.assertFalse(query.flags & QUERY_FLAGS['SlaveOkay'],
                             'SlaveOkay set with topology type Single')

    def test_standalone(self):
        self.server.autoresponds('ismaster')
        self._test_slave_okay(True)

    def test_slave(self):
        self.server.autoresponds('ismaster', ismaster=False)
        self._test_slave_okay(True)

    def test_mongos(self):
        self.server.autoresponds('ismaster', msg='isdbgrid')
        self._test_slave_okay(False)


class TestSlaveOkaySharded(unittest.TestCase):
    def test_two_mongoses(self):
        server1, server2 = MockupDB(), MockupDB()

        # Collect queries to either server in one queue.
        q = Queue()
        for server in server1, server2:
            server.subscribe(q.put)
            server.run()
            self.addCleanup(server.stop)
            server.autoresponds('ismaster', msg='isdbgrid')

        mongoses_uri = 'mongodb://%s,%s' % (server1.address_string,
                                            server2.address_string)

        client = MongoClient(mongoses_uri)
        with going(client.db.collection.find_one):
            query = q.get(timeout=10)
            query.reply({'one': 'doc'})

        self.assertEqual(topology_type_name(client), 'Sharded')
        self.assertFalse(query.flags & QUERY_FLAGS['SlaveOkay'])

if __name__ == '__main__':
    unittest.main()
