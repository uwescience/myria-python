from httmock import urlmatch, HTTMock
from json import dumps as jstr
import unittest
from myria import MyriaConnection


@urlmatch(netloc=r'localhost:8753')
def local_mock(url, request):
    global query_counter
    if url.path == '/workers':
        return jstr({'1': 'localhost:9001', '2': 'localhost:9002'})
    elif url.path == '/workers/alive':
        return jstr([1, 2])
    elif url.path == '/workers/worker-1':
        return jstr("localhost:9001")

    return None


class TestQuery(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=8753)
        unittest.TestCase.__init__(self, args)

    def test_workers(self):
        with HTTMock(local_mock):
            workers = self.connection.workers()
            self.assertEquals(workers, {'1': 'localhost:9001',
                                        '2': 'localhost:9002'})

    def test_alive(self):
        with HTTMock(local_mock):
            workers = self.connection.workers_alive()
            self.assertEquals(set(workers), set([1, 2]))

    def test_worker_1(self):
        with HTTMock(local_mock):
            worker = self.connection.worker(1)
            self.assertEquals(worker, 'localhost:9001')
