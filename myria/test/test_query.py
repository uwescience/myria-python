from httmock import urlmatch, HTTMock
import json
import unittest
from myria import MyriaConnection


def jstr(obj):
    """The JSON string representation of the object"""
    return json.dumps(obj)


def query():
    """Simple empty query"""
    return {'rawQuery': 'empty',
            'logicalRa': 'empty',
            'fragments': []}


def query_status(query, query_id=17, status='OK'):
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


@urlmatch(netloc=r'localhost:8753')
def local_mock(url, request):
    if url.path == '/workers':
        return jstr({'1': 'localhost:9001', '2': 'localhost:9002'})
    elif url.path == '/query':
        body = query_status(query(), 17, 'ACCEPTED')
        headers = {'Location': 'http://localhost:8753/query/query-17'}
        return {'status_code': 202, 'content': body, 'headers': headers}
    elif url.path == '/query/query-17':
        body = query_status(query(), 17, 'OK')
        headers = {'Location': 'http://localhost:8753/query/query-17'}
        return {'status_code': 201, 'content': body, 'headers': headers}
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
            self.assertEquals(status, query_status(q))

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
