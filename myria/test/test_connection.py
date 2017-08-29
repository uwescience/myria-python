from httmock import urlmatch, HTTMock
import json
import unittest
from myria import MyriaConnection, MyriaRelation, MyriaSchema


@urlmatch(netloc=r'localhost:12345')
def local_mock(url, request):
    ret = None
    if url.path == '/workers':
        ret = {'1': 'localhost:12347', '2': 'localhost:12348'}
    elif url.path == '/function':
        ret = {}

    return json.dumps(ret)


class TestDefaultConnection(unittest.TestCase):
    def test_default_connection_assignment(self):
        self.assertIsNotNone(MyriaConnection.DefaultConnection)
        #self.assertEqual(MyriaRelation.DefaultConnection,
        #                 MyriaConnection.DefaultConnection)

        connection = MyriaConnection(rest_url='http://foo.bar')
        MyriaConnection.DefaultConnection = connection

        self.assertEqual(MyriaConnection.DefaultConnection, connection)
        self.assertEqual(MyriaRelation.DefaultConnection, connection)

    def test_default_connection_application(self):
        with HTTMock(local_mock):
            connection = MyriaConnection(hostname='localhost', port=12345)
            MyriaConnection.DefaultConnection = connection
            relation = MyriaRelation('relation',
                          schema=MyriaSchema({'columnNames': ['foo'],
                                              'columnTypes': ['INT_TYPE']}))
            self.assertEqual(relation.connection, connection)


class TestDeployment(unittest.TestCase):
    def test_no_deployment(self):
        assert MyriaConnection._parse_deployment(None) is None

    def test_parse_deploy_file(self):
        with open('myria/test/deployment.cfg.local') as deploy_file:
            hostname, port = MyriaConnection._parse_deployment(deploy_file)
            self.assertEqual(hostname, 'localhost')
            self.assertEqual(port, 12345)

    def test_deploy_file(self):
        with HTTMock(local_mock):
            connection = None
            with open('myria/test/deployment.cfg.local') as deploy_file:
                connection = MyriaConnection(deploy_file)
            assert connection is not None

            self.assertEquals(connection.workers(),
                              {'1': 'localhost:12347', '2': 'localhost:12348'})

    def test_deploy_params(self):
        with HTTMock(local_mock):
            connection = MyriaConnection(hostname='localhost', port=12345)
            assert connection is not None

            self.assertEquals(connection.workers(),
                              {'1': 'localhost:12347', '2': 'localhost:12348'})
