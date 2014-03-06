from httmock import urlmatch, HTTMock
import unittest
from myria import MyriaConnection


@urlmatch(netloc=r'localhost:12345')
def local_mock(url, request):
    if url.path == '/logs/sent':
        body = 'foo,bar\nbaz,ban'
        return {'status_code': 200, 'content': body}
    return None


class TestLogs(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        unittest.TestCase.__init__(self, args)

    def test_sent_logs(self):
        with HTTMock(local_mock):
            logs = self.connection.get_sent_logs(42)
            self.assertEquals(list(logs), [['foo', 'bar'], ['baz', 'ban']])
