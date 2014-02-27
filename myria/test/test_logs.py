from httmock import urlmatch, HTTMock
import unittest
from myria import MyriaConnection


@urlmatch(netloc=r'localhost:8753')
def local_mock(url, request):
    print url
    if url.path == '/logs/sent':
        body = 'foo,bar\nbaz,ban'
        return {'status_code': 200, 'content': body}

    return None


class TestQuery(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=8753)
        unittest.TestCase.__init__(self, args)

    def test_sent_logs(self):
        with HTTMock(local_mock):
            logs = self.connection.get_sent_logs(42)
            self.assertEquals(list(logs), [['foo', 'bar'], ['baz', 'ban']])
