# Copyright 2017-present MongoDB, Inc.
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

"""Test $clusterTime handling."""

from mockupdb._bson import Timestamp
from mockupdb import going, MockupDB
from pymongo import MongoClient, InsertOne, UpdateOne, DeleteMany

from tests import unittest


class TestClusterTime(unittest.TestCase):
    def cluster_time_conversation(self, callback, replies):
        cluster_time = Timestamp(0, 0)
        server = MockupDB()
        server.autoresponds('ismaster',
                            {'minWireVersion': 2,
                             'maxWireVersion': 6,
                             '$clusterTime': {'clusterTime': cluster_time}})
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)
        self.addCleanup(client.close)

        with going(callback, client) as future:
            for reply in replies:
                request = server.receives()
                self.assertIn('$clusterTime', request)
                self.assertEqual(request['$clusterTime']['clusterTime'],
                                 cluster_time)
                cluster_time = Timestamp(cluster_time.time,
                                         cluster_time.inc + 1)
                reply['$clusterTime'] = {'clusterTime': cluster_time}
                request.reply(reply)

        future()

    def test_command(self):
        def callback(client):
            client.db.command('ping')
            client.db.command('ping')

        self.cluster_time_conversation(callback, [{'ok': 1}] * 2)

    def test_bulk(self):
        def callback(client):
            client.db.collection.bulk_write([
                InsertOne({}),
                InsertOne({}),
                UpdateOne({}, {'$inc': {'x': 1}}),
                DeleteMany({})])

        self.cluster_time_conversation(callback, [{'ok': 1}] * 3)

    batches = [
        {'cursor': {'id': 123, 'firstBatch': [{'a': 1}]}},
        {'cursor': {'id': 123, 'nextBatch': [{'a': 2}]}},
        {'cursor': {'id': 0, 'nextBatch': [{'a': 3}]}}]

    def test_cursor(self):
        def callback(client):
            list(client.db.collection.find())

        self.cluster_time_conversation(callback, self.batches)

    def test_aggregate(self):
        def callback(client):
            list(client.db.collection.aggregate([]))

        self.cluster_time_conversation(callback, self.batches)


if __name__ == '__main__':
    unittest.main()
