from httmock import urlmatch, HTTMock
from json import dumps as jstr
import unittest
from myria import MyriaConnection


def query():
    """Simple empty query"""
    return {'rawQuery': 'empty',
            'logicalRa': 'empty',
            'fragments': []}


def query_status(query, query_id=17, status='SUCCESS'):
    return {'url': 'http://localhost:8753/query/query-%d' % query_id,
            'queryId': query_id,
            'rawQuery': query['rawQuery'],
            'logicalRa': query['rawQuery'],
            'physicalPlan': query,
            'submitTime': '2014-02-26T15:19:54.505-08:00',
            'startTime': '2014-02-26T15:19:54.611-08:00',
            'finishTime': '2014-02-26T15:23:34.189-08:00',
            'elapsedNanos': 219577567891,
            'status': status}


query_counter = 0


@urlmatch(netloc=r'localhost:8753')
def local_mock(url, request):
    global query_counter
    if url.path == '/workers':
        return jstr({'1': 'localhost:9001', '2': 'localhost:9002'})
    elif url.path == '/query':
        body = query_status(query(), 17, 'ACCEPTED')
        headers = {
            'Location': 'http://localhost:8753/query/query-17',
            'Count': 42}
        query_counter = 2
        return {'status_code': 202, 'content': body, 'headers': headers}
    elif url.path == '/query/query-17':
        if query_counter == 0:
            status = 'SUCCESS'
            status_code = 201
        else:
            status = 'ACCEPTED'
            status_code = 202
            query_counter -= 1
        body = query_status(query(), 17, status)
        headers = {'Location': 'http://localhost:8753/query/query-17'}
        return {'status_code': status_code,
                'content': body,
                'headers': headers}
    elif url.path == '/query/validate':
        return request.body

    return None


class TestQuery(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=8753)
        unittest.TestCase.__init__(self, args)

    def test_submit(self):
        q = query()
        with HTTMock(local_mock):
            status = self.connection.submit_query(q)
            self.assertEquals(status, query_status(q, status='ACCEPTED'))
            self.assertEquals(query_counter, 1)

    def test_execute(self):
        q = query()
        with HTTMock(local_mock):
            status = self.connection.execute_query(q)
            self.assertEquals(status, query_status(q))

    def test_validate(self):
        q = query()
        with HTTMock(local_mock):
            validated = self.connection.validate_query(q)
            self.assertEquals(validated, q)

    def test_query_status(self):
        q = query()
        with HTTMock(local_mock):
            status = self.connection.get_query_status(17)
            self.assertEquals(status, query_status(q))

    def x_test_queries(self):
        with HTTMock(local_mock):
            count, _ = self.connection.queries()
            self.assertEquals(42, count)
