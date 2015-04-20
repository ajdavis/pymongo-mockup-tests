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

"""Test PyMongo query and read preference with a sharded cluster."""

from pymongo import MongoClient
from pymongo.read_preferences import (Primary,
                                      PrimaryPreferred,
                                      Secondary,
                                      SecondaryPreferred,
                                      Nearest)
from mockupdb import MockupDB, going, OpQuery

from tests import unittest


class TestQueryAndReadModeSharded(unittest.TestCase):
    def test_query_and_read_mode_sharded(self):
        server = MockupDB()
        server.autoresponds('ismaster', ismaster=True, msg='isdbgrid')
        server.run()
        self.addCleanup(server.stop)

        client = MongoClient(server.uri)

        modes_without_query = (
            Primary(),
            SecondaryPreferred(),)

        modes_with_query = (
            PrimaryPreferred(),
            Secondary(),
            Nearest(),
            SecondaryPreferred([{'tag': 'value'}]),)

        for query in ({'a': 1}, {'$query': {'a': 1}},):
            for mode in modes_with_query + modes_without_query:
                collection = client.db.get_collection('test',
                                                      read_preference=mode)
                cursor = collection.find(query.copy())
                with going(next, cursor):
                    request = server.receives(OpQuery)
                    if mode in modes_without_query:
                        # Query is not edited: {'a': 1} is not nested in $query,
                        # {'$query': {'a': 1}} is not hoisted.
                        request.assert_matches(query)
                        self.assertFalse('$readPreference' in request)
                    else:
                        # {'a': 1} is *always* nested in $query.
                        request.assert_matches({
                            '$query': {'a': 1},
                            '$readPreference': mode.document
                        })

                    request.replies({})


if __name__ == '__main__':
    unittest.main()
