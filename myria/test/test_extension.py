import unittest
from collections import namedtuple
from IPython.config.loader import KeyValueConfigLoader
from myria import extension


def get_shell():
    fields = ['config', 'configurables', 'user_ns']
    values = KeyValueConfigLoader().load_config([]), [], {}
    return namedtuple('ShellMock', fields)(*values)


class TestExtension(unittest.TestCase):
    def test_execute_line(self):
        language = u'Elven'
        url = u'http://foo.bar:80'
        line = u'{}, {}'.format(language, url)

        ext = extension.MyriaExtension(shell=get_shell())
        ext.execute(line)

        self.assertEqual(ext.ambient_connection._url_start, url)
        self.assertEqual(ext.language, language)

    def test_create_connection(self):
        url = u'http://foo.bar:80'
        connection = extension._create_connection(url)
        self.assertEqual(connection._url_start, url)

    def test_bind(self):
        query = 'foo'
        self.assertEqual(query, extension._bind(query, {}))

        query = 'foo :bar baz'
        expected = 'foo 999 baz'
        self.assertEqual(expected, extension._bind(query, {'bar': 999}))
