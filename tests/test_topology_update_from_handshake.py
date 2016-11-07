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

from mockupdb import MockupDB
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from tests import unittest


class TestTopologyUpdateFromHandshake(unittest.TestCase):
    def test_server_removed(self):
        server = MockupDB()
        server.run()
        self.addCleanup(server.stop)

        # Single-node RS.
        server.autoresponds('ismaster', ismaster=True, setName='rs',
                            hosts=[server.address_string])

        server.autoresponds('ping')

        client = MongoClient(server.uri,
                             replicaSet='rs',
                             serverSelectionTimeoutMS=100,
                             heartbeatFrequencyMS=9999999)

        # Succeeds.
        client.admin.command('ping')

        # Changes setName.
        server.autoresponds('ismaster', ismaster=True, setName='BAD NAME',
                            hosts=[server.address_string])

        pool = client._topology.get_server_by_address(server.address)._pool
        pool.reset()

        with self.assertRaises(ConnectionFailure) as ctx:
            client.admin.command('ping')

        self.assertIn('removed from topology', ctx.exception.message)


if __name__ == '__main__':
    unittest.main()
