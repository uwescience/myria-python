from datetime import datetime
import json
from httmock import urlmatch, HTTMock
import unittest
import requests
from myria.connection import MyriaConnection
from myria.schema import MyriaSchema
from myria.relation import MyriaRelation
from myria.query import MyriaQuery
from test_connection_query import query_status

QUERY_ID = -1
RUNNING_QUERY_ID = 999
COMPLETED_QUERY_ID = 998

RAW_QUERY = {'rawQuery': 'foo'}
RELATION_NAME = 'relation'
FULL_NAME = 'public:adhoc:' + RELATION_NAME
QUALIFIED_NAME = {'userName': 'public',
                  'programName': 'adhoc',
                  'relationName': RELATION_NAME}
NAME_COMPONENTS = ['public', 'adhoc', RELATION_NAME]
QUERY_TIME = datetime(1900, 1, 2, 3, 4)
TUPLES = [[1], [2], [3], [4], [5]]


STATE_SUCCESS = 'Unittest-Success'
STATE_RUNNING = 'RUNNING'


def get_query_dataset(query_id):
    return [{'relationKey': QUALIFIED_NAME,
             'schema': {
                 'columnNames': ['column'],
                 'columnTypes': ['INT_TYPE']},
             'numTuples': 1,
             'queryId': query_id,
             'created': str(QUERY_TIME)}]


@urlmatch(netloc=r'localhost:12345')
def local_mock(url, request):
    # Query metadata
    if url.path == '/query/query-{}'.format(QUERY_ID):
        body = query_status(RAW_QUERY, query_id=QUERY_ID)
        return {'status_code': 200, 'content': body}

    elif url.path == '/query/query-{}'.format(COMPLETED_QUERY_ID):
        body = query_status(RAW_QUERY, query_id=QUERY_ID, status=STATE_SUCCESS)
        return {'status_code': 200, 'content': body}

    elif url.path == '/query/query-{}'.format(RUNNING_QUERY_ID):
        body = query_status(RAW_QUERY,
                            query_id=RUNNING_QUERY_ID,
                            status=STATE_RUNNING)
        return {'status_code': 200, 'content': body}

    # Dataset metadata
    elif url.path == '/dataset':
        body = json.dumps(get_query_dataset(QUERY_ID))
        return {'status_code': 200, 'content': body}

    elif url.path == '/dataset/user-public/program-adhoc' \
                     '/relation-relation':
        body = str(TUPLES)
        return {'status_code': 404, 'content': body}

    elif url.path == '/dataset/user-public/program-adhoc' \
                     '/relation-relation/data':
        body = str(TUPLES)
        return {'status_code': 200, 'content': body}

    # Query submission
    elif url.path == '/query':
        return {'status_code': 201,
                'content': '',
                'headers': [('Location', '/query-submitted-uri')]}

    elif url.path == '/query-submitted-uri':
        body = json.dumps({'queryId': RUNNING_QUERY_ID})
        return {'status_code': 201,
                'content': body,
                'headers': [('Location', '/query-submitted-uri')]}

    return None


class TestQuery(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        super(TestQuery, self).__init__(args)

    def test_id(self):
        with HTTMock(local_mock):
            query = MyriaQuery(QUERY_ID, connection=self.connection)
            self.assertEqual(query.query_id, QUERY_ID)

    def test_connection(self):
        with HTTMock(local_mock):
            query = MyriaQuery(QUERY_ID, connection=self.connection)
            self.assertEqual(query.connection, self.connection)

            query = MyriaQuery(QUERY_ID)
            self.assertEqual(query.connection._url_start,
                             MyriaRelation.DefaultConnection._url_start)
            self.assertEqual(query.connection.execution_url,
                             MyriaRelation.DefaultConnection.execution_url)

    def test_timeout(self):
        timeout = 999
        with HTTMock(local_mock):
            query = MyriaQuery(QUERY_ID,
                               connection=self.connection,
                               timeout=timeout)
            self.assertEqual(query.timeout, timeout)

    def test_wait_for_completion(self):
        with HTTMock(local_mock):
            query = MyriaQuery(COMPLETED_QUERY_ID,
                               connection=self.connection,
                               wait_for_completion=True,
                               timeout=1)
            self.assertEqual(query.status, STATE_SUCCESS)

            self.assertRaises(requests.Timeout,
                              MyriaQuery,
                              RUNNING_QUERY_ID,
                              connection=self.connection,
                              wait_for_completion=True,
                              timeout=1)

            query = MyriaQuery(RUNNING_QUERY_ID,
                               connection=self.connection,
                               wait_for_completion=False,
                               timeout=1)
            self.assertRaises(requests.Timeout,
                              query.wait_for_completion)

    def test_name(self):
        with HTTMock(local_mock):
            query = MyriaQuery(COMPLETED_QUERY_ID, connection=self.connection)
            self.assertEqual(query.name, FULL_NAME)
            self.assertEqual(query.qualified_name, QUALIFIED_NAME)
            self.assertEqual(query.components, NAME_COMPONENTS)

    def test_status(self):
        with HTTMock(local_mock):
            query = MyriaQuery(COMPLETED_QUERY_ID, connection=self.connection)
            self.assertEqual(query.status, STATE_SUCCESS)

            query = MyriaQuery(RUNNING_QUERY_ID, connection=self.connection)
            self.assertEqual(query.status, STATE_RUNNING)

    def test_dict(self):
        with HTTMock(local_mock):
            query = MyriaQuery(COMPLETED_QUERY_ID, connection=self.connection)
            self.assertEqual(query.to_dict(), TUPLES)

            query = MyriaQuery(RUNNING_QUERY_ID,
                               connection=self.connection,
                               timeout=1)
            self.assertRaises(requests.Timeout,
                              query.to_dict)

    def test_submit_plan(self):
        with HTTMock(local_mock):
            plan = 'This is a Myria JSON plan'
            query = MyriaQuery.submit_plan(plan, connection=self.connection)
            self.assertEquals(query.status, STATE_RUNNING)

    def test_parallel_import(self):
        with HTTMock(local_mock):
            schema = MyriaSchema({'columnNames': ['column'],
                                  'columnTypes': ['INT_TYPE']})
            relation = MyriaRelation(FULL_NAME,
                                     schema=schema,
                                     connection=self.connection)
            work = [('http://input-uri-0', 0), ('http://input-uri-1', 1)]

            query = MyriaQuery.parallel_import(relation, work)
            self.assertEquals(query.status, STATE_RUNNING)
