# Copyright 2018-present MongoDB, Inc.
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

from collections import namedtuple

from mockupdb import MockupDB, going, OpMsg, OP_MSG_FLAGS
from pymongo import MongoClient, WriteConcern, version_tuple
from pymongo.operations import InsertOne, UpdateOne, DeleteOne

from tests import unittest


Operation = namedtuple(
    'Operation',
    ['name', 'function', 'request', 'reply'])

operations = [
    Operation(
        'insert_one',
        lambda coll: coll.insert_one({}),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1}),
    Operation(
        'insert_one-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).insert_one({}),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'insert_many',
        lambda coll: coll.insert_many([{}, {}, {}]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 3}),
    Operation(
        'insert_many-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).insert_many([{}, {}, {}]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 3}),
    Operation(
        'insert_many-w0-unordered',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).insert_many(
                [{}, {}, {}], ordered=False),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'replace_one',
        lambda coll: coll.replace_one({"_id": 1}, {"new": 1}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1, 'nModified': 1}),
    Operation(
        'replace_one-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).replace_one({"_id": 1},
                                                         {"new": 1}),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'update_one',
        lambda coll: coll.update_one({"_id": 1}, {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1, 'nModified': 1}),
    Operation(
        'replace_one-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).update_one({"_id": 1},
                                                        {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'update_many',
        lambda coll: coll.update_many({"_id": 1}, {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1, 'nModified': 1}),
    Operation(
        'update_many-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).update_many({"_id": 1},
                                                         {"$set": {"new": 1}}),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'delete_one',
        lambda coll: coll.delete_one({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1}),
    Operation(
        'delete_one-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).delete_one({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'delete_many',
        lambda coll: coll.delete_many({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1}),
    Operation(
        'delete_many-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).delete_many({"a": 1}),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    # Legacy methods
    Operation(
        'insert',
        lambda coll: coll.insert({}),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1}),
    Operation(
        'insert-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).insert({}),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'insert-w0-argument',
        lambda coll: coll.insert({}, w=0),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'update',
        lambda coll: coll.update({"_id": 1}, {"new": 1}),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1, 'nModified': 1}),
    Operation(
        'update-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).update({"_id": 1}, {"new": 1}),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'update-w0-argument',
        lambda coll: coll.update({"_id": 1}, {"new": 1}, w=0),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'remove',
        lambda coll: coll.remove({"_id": 1}),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={'ok': 1, 'n': 1}),
    Operation(
        'remove-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).remove({"_id": 1}),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'remove-w0-argument',
        lambda coll: coll.remove({"_id": 1}, w=0),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'bulk_write_insert',
        lambda coll: coll.bulk_write([InsertOne({}), InsertOne({})]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2}),
    Operation(
        'bulk_write_insert-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write([InsertOne({}),
                                                         InsertOne({})]),
        request=OpMsg({"insert": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2}),
    Operation(
        'bulk_write_insert-w0-unordered',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write(
            [InsertOne({}), InsertOne({})], ordered=False),
        request=OpMsg({"insert": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'bulk_write_update',
        lambda coll: coll.bulk_write([
            UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
            UpdateOne({"_id": 2}, {"$set": {"new": 1}})]),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2, 'nModified': 2}),
    Operation(
        'bulk_write_update-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write([
                UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
                UpdateOne({"_id": 2}, {"$set": {"new": 1}})]),
        request=OpMsg({"update": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2, 'nModified': 2}),
    Operation(
        'bulk_write_update-w0-unordered',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write([
                UpdateOne({"_id": 1}, {"$set": {"new": 1}}),
                UpdateOne({"_id": 2}, {"$set": {"new": 1}})], ordered=False),
        request=OpMsg({"update": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
    Operation(
        'bulk_write_delete',
        lambda coll: coll.bulk_write([
            DeleteOne({"_id": 1}), DeleteOne({"_id": 2})]),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2}),
    Operation(
        'bulk_write_delete-w0',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write([
                DeleteOne({"_id": 1}), DeleteOne({"_id": 2})]),
        request=OpMsg({"delete": "coll"}, flags=0),
        reply={'ok': 1, 'n': 2}),
    Operation(
        'bulk_write_delete-w0-unordered',
        lambda coll: coll.with_options(
            write_concern=WriteConcern(w=0)).bulk_write([
                DeleteOne({"_id": 1}), DeleteOne({"_id": 2})], ordered=False),
        request=OpMsg({"delete": "coll"}, flags=OP_MSG_FLAGS['moreToCome']),
        reply=None),
]


class TestOpMsg(unittest.TestCase):

    @unittest.skipUnless(version_tuple >= (3, 7), "requires PyMongo 3.7")
    @classmethod
    def setUpClass(cls):
        cls.server = MockupDB(auto_ismaster=True)
        cls.server.run()
        cls.client = MongoClient(cls.server.uri)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.client.close()

    def _test_operation(self, op):
        coll = self.client.db.coll
        with going(op.function, coll) as future:
            request = self.server.receives()
            request.assert_matches(op.request)
            if op.reply is not None:
                request.reply(op.reply)

        future()  # No error.


def operation_test(op):
    def test(self):
        self._test_operation(op)
    return test


def create_tests():
    for op in operations:
        test_name = "test_op_msg_%s" % (op.name,)

        setattr(TestOpMsg, test_name, operation_test(op))


create_tests()

if __name__ == '__main__':
    unittest.main()
