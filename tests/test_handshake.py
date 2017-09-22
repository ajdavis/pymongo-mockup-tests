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


from mockupdb import MockupDB, OpReply, absent, Command, go
from pymongo import MongoClient, version as pymongo_version, version_tuple

from tests import unittest


def _check_handshake_data(request):
    assert 'client' in request
    data = request['client']

    assert data['application'] == {'name': 'my app'}
    assert data['driver'] == {'name': 'PyMongo', 'version': pymongo_version}

    # Keep it simple, just check these fields exist.
    assert 'os' in data
    assert 'platform' in data


class TestHandshake(unittest.TestCase):
    @unittest.skipUnless(version_tuple >= (3, 4), "requires PyMongo 3.4")
    def test_client_handshake_data(self):
        primary, secondary = MockupDB(), MockupDB()
        for server in primary, secondary:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in primary, secondary]
        primary_response = OpReply('ismaster', True,
                                   setName='rs', hosts=hosts,
                                   minWireVersion=2, maxWireVersion=6)

        secondary_response = OpReply('ismaster', False,
                                     setName='rs', hosts=hosts,
                                     secondary=True,
                                     minWireVersion=2, maxWireVersion=6)

        client = MongoClient(primary.uri,
                             replicaSet='rs',
                             appname='my app',
                             heartbeatFrequencyMS=500)  # Speed up the test.

        self.addCleanup(client.close)

        # New monitoring sockets send data during handshake.
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(primary_response)

        heartbeat = secondary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(secondary_response)

        # Subsequent heartbeats have no client data.
        primary.receives('ismaster', 1, client=absent).ok(primary_response)
        secondary.receives('ismaster', 1, client=absent).ok(secondary_response)

        # After a disconnect, next ismaster has client data again.
        primary.receives('ismaster', 1, client=absent).hangup()
        heartbeat = primary.receives('ismaster')
        _check_handshake_data(heartbeat)
        heartbeat.ok(primary_response)

        secondary.autoresponds('ismaster', secondary_response)

        # Start a command, so the client opens an application socket.
        future = go(client.db.command, 'whatever')

        for request in primary:
            if request.matches(Command('ismaster')):
                if request.client_port == heartbeat.client_port:
                    # This is the monitor again, keep going.
                    request.ok(primary_response)
                else:
                    # Handshaking a new application socket.
                    _check_handshake_data(heartbeat)
                    request.ok(primary_response)
            else:
                # Command succeeds.
                request.assert_matches(Command('whatever'))
                request.ok()
                assert future()
                return

if __name__ == '__main__':
    unittest.main()
