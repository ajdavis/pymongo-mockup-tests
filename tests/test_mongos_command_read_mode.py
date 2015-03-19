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

from mockupdb import MockupDB, going
from pymongo import MongoClient, ReadPreference

from tests import unittest


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

if __name__ == '__main__':
    unittest.main()
