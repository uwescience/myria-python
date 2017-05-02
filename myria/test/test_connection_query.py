from httmock import urlmatch, HTTMock
import unittest
from myria import MyriaConnection
from myria.test.mock import create_mock, FULL_NAME, FULL_NAME2, UDF1_ARITY, \
    UDF1_TYPE, SCHEMA


def query():
    """Simple empty query"""
    return {'rawQuery': 'empty',
            'logicalRa': 'empty',
            'fragments': []}


def query_status(query, query_id=17, status='SUCCESS'):
    return {'url': 'http://localhost:12345/query/query-%d' % query_id,
            'queryId': query_id,
            'rawQuery': query['rawQuery'],
            'logicalRa': query['rawQuery'],
            'plan': query,
            'submitTime': '2014-02-26T15:19:54.505-08:00',
            'startTime': '2014-02-26T15:19:54.611-08:00',
            'finishTime': '2014-02-26T15:23:34.189-08:00',
            'elapsedNanos': 219577567891,
            'status': status}


query_counter = 0


class TestQuery(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(create_mock()):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        unittest.TestCase.__init__(self, args)

    def test_submit(self):
        q = query()
        with HTTMock(create_mock()):
            status = self.connection.submit_query(q)
            self.assertEquals(status, query_status(q, status='ACCEPTED'))
            self.assertEquals(query_counter, 1)

    def test_execute(self):
        q = query()
        with HTTMock(create_mock()):
            status = self.connection.execute_query(q)
            self.assertEquals(status, query_status(q))

    def test_compile_plan(self):
        with HTTMock(create_mock()):
            myrial = "a = empty(i:int);\nstore(a, a);"
            json = self.connection.compile_program(myrial, language="MyriaL")
            self.assertEqual(json['rawQuery'], myrial)

    def test_validate(self):
        q = query()
        with HTTMock(create_mock()):
            validated = self.connection.validate_query(q)
            self.assertEquals(validated, q)

    def test_query_status(self):
        q = query()
        with HTTMock(create_mock()):
            status = self.connection.get_query_status(17)
            self.assertEquals(status, query_status(q))

    def x_test_queries(self):
        with HTTMock(create_mock()):
            result = self.connection.queries()
            self.assertEquals(result['max'], 17)
            self.assertEquals(result['min'], 1)
            self.assertEquals(result['results'][0]['queryId'], 17)
