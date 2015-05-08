import unittest
from myria import extension

try:
    import IPython
except ImportError:
    IPython = None

if IPython:
    class TestExtension(unittest.TestCase):
        def test_connect(self):
            if IPython is None:
                return

            rest_url = u'http://foo.bar:80'
            execution_url = u'http://baz.qux:999'
            language = 'Elven'
            timeout = 999

            ext = extension.MyriaExtension(shell=IPython.InteractiveShell())

            connection = ext.connect(rest_url)
            self.assertEqual(connection._url_start, rest_url)

            line = u'{} {}'.format(rest_url, execution_url)
            connection = ext.connect(line)
            self.assertEqual(connection._url_start, rest_url)
            self.assertEqual(connection.execution_url, execution_url)

            line = u'{} {} -l Elven -t 999'.format(rest_url, execution_url)
            connection = ext.connect(line)
            self.assertEqual(connection._url_start, rest_url)
            self.assertEqual(connection.execution_url, execution_url)
            self.assertEqual(ext.language, language)
            self.assertEqual(ext.timeout, timeout)

        def test_bind(self):
            query = 'foo'
            self.assertEqual(query, extension._bind(query, {}))

            query = 'foo @bar baz'
            expected = 'foo 999 baz'
            self.assertEqual(expected, extension._bind(query, {'bar': 999}))
