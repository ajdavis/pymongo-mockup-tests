# Copyright 2016 MongoDB, Inc.
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

from datetime import datetime, timedelta

from mockupdb import MockupDB, going, wait_until
from pymongo import MongoClient
from pymongo.errors import ConfigurationError

from tests import unittest


# Initial query with maxStalenessSeconds and non-default idleWritePeriodMS.
class TestIdleWritePeriod(unittest.TestCase):
    def test_with_primary(self):
        primary, secondary = MockupDB(), MockupDB()
        for server in primary, secondary:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in primary, secondary]
        now = datetime.now()

        # Wire version 5 required for max staleness. Set idle period to 1 sec.
        primary.autoresponds(
            'ismaster',
            idleWritePeriodMillis=1000, ismaster=True, setName='rs',
            hosts=hosts, maxWireVersion=5, lastWrite={'lastWriteDate': now})

        secondary.autoresponds(
            'ismaster',
            idleWritePeriodMillis=1000, ismaster=False, secondary=True,
            setName='rs', hosts=hosts, maxWireVersion=5,
            lastWrite={'lastWriteDate': now})

        # Set maxStalenessSeconds to 1.5.
        uri = 'mongodb://localhost:%d,localhost:%d/?replicaSet=rs' \
              '&readPreference=secondary&maxStalenessSeconds=1.5' % (
                primary.port, secondary.port)

        # Default heartbeat is 10 sec, so max staleness must be >= 11 sec.
        with self.assertRaises(ConfigurationError) as ctx:
            MongoClient(uri).db.coll.find_one()

        self.assertIn(
            'maxStalenessSeconds must be at least heartbeatFrequencyMS',
            ctx.exception.message)

        # Set heartbeat to 500ms, max staleness must be >= 1.5 sec.
        client = MongoClient(uri, heartbeatFrequencyMS=500)
        wait_until(lambda: len(client.nodes) == 2, 'discover both nodes')
        with going(client.db.coll.find_one) as future:
            secondary.receives(find='coll').ok(cursor={'firstBatch': [],
                                                       'id': 0})

        # find_one succeeds with no result.
        self.assertIsNone(future())

    def test_without_primary(self):
        fresh, stale = MockupDB(), MockupDB()
        for server in fresh, stale:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in fresh, stale]
        now = datetime.now()

        # Wire version 5 required for max staleness. Set idle period to 1 sec.
        fresh.autoresponds(
            'ismaster',
            idleWritePeriodMillis=1000, ismaster=False, secondary=True,
            setName='rs', hosts=hosts, maxWireVersion=5,
            lastWrite={'lastWriteDate': now})

        stale.autoresponds(
            'ismaster',
            idleWritePeriodMillis=1000, ismaster=False, secondary=True,
            setName='rs', hosts=hosts, maxWireVersion=5,
            lastWrite={'lastWriteDate': now - timedelta(seconds=2)})

        # Set maxStalenessSeconds to 1.5.
        uri = 'mongodb://localhost:%d,localhost:%d/?replicaSet=rs' \
              '&readPreference=secondary&maxStalenessSeconds=1.5' % (
                  fresh.port, stale.port)

        # Default heartbeat is 10 sec, so max staleness must be >= 11 sec.
        with self.assertRaises(ConfigurationError) as ctx:
            MongoClient(uri).db.coll.find_one()

        self.assertIn(
            'maxStalenessSeconds must be at least heartbeatFrequencyMS',
            ctx.exception.message)

        # Set heartbeat to 500ms, max staleness must be >= 1.5 sec.
        client = MongoClient(uri, heartbeatFrequencyMS=500)
        wait_until(lambda: len(client.nodes) == 2, 'discover both nodes')
        with going(client.db.coll.find_one) as future:
            fresh.receives(find='coll').ok(cursor={'firstBatch': [], 'id': 0})

        # find_one succeeds with no result.
        self.assertIsNone(future())


class TestMaxStalenessMongos(unittest.TestCase):
    def test_mongos(self):
        mongos = MockupDB()
        mongos.autoresponds('ismaster', maxWireVersion=5,
                            ismaster=True, msg='isdbgrid')
        mongos.run()
        self.addCleanup(mongos.stop)

        # No maxStalenessSeconds.
        uri = 'mongodb://localhost:%d/?readPreference=secondary' % mongos.port

        with going(MongoClient(uri).db.coll.find_one) as future:
            request = mongos.receives()
            self.assertNotIn(
                'maxStalenessSeconds',
                request.doc['$readPreference'])

            self.assertTrue(request.slave_okay)
            request.ok(cursor={'firstBatch': [], 'id': 0})

        # find_one succeeds with no result.
        self.assertIsNone(future())

        # Set maxStalenessSeconds to 1.5. Client has no minimum with mongos.
        uri = 'mongodb://localhost:%d/?readPreference=secondary' \
              '&maxStalenessSeconds=1.5' % mongos.port

        with going(MongoClient(uri).db.coll.find_one) as future:
            request = mongos.receives()
            self.assertAlmostEqual(
                1.5,
                request.doc['$readPreference']['maxStalenessSeconds'])

            self.assertTrue(request.slave_okay)
            request.ok(cursor={'firstBatch': [], 'id': 0})

        self.assertIsNone(future())


if __name__ == '__main__':
    unittest.main()
