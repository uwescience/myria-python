from httmock import urlmatch, HTTMock
from json import dumps as jstr, loads
from myria.cmd import upload_file
from myria.errors import MyriaError
import unittest
import sys


class NullWriter:
    def write(self, s):
        pass


class QuietStderr:
    def __enter__(self):
        self.old_stderr = sys.stderr
        sys.stderr = NullWriter()

    def __exit__(self, type, value, traceback):
        sys.stderr = self.old_stderr


class TestCmd(unittest.TestCase):
    def test_parse_bad_args(self):
        with QuietStderr():
            # Missing one or both required arguments
            with self.assertRaises(SystemExit):
                args = upload_file.parse_args()
            with self.assertRaises(SystemExit):
                args = upload_file.parse_args(['--relation', 'tmp'])
            with self.assertRaises(SystemExit):
                try:
                    args = upload_file.parse_args(['nosuchfile'])
                except IOError:
                    raise SystemExit()

            # Illegal file
            with self.assertRaises(SystemExit):
                try:
                    args = upload_file.parse_args(['--relation', 'tmp',
                                                   'nosuchfile'])
                except IOError:
                    raise SystemExit()

            # Bad port
            with self.assertRaises(SystemExit):
                args = upload_file.parse_args(['--relation', 'tmp',
                                               '--port', 'abc',
                                               'testdata/TwitterK.csv'])
            with self.assertRaises(SystemExit):
                args = upload_file.parse_args(['--relation', 'tmp',
                                               '--port', '-1',
                                               'testdata/TwitterK.csv'])
            with self.assertRaises(SystemExit):
                args = upload_file.parse_args(['--relation', 'tmp',
                                               '--port', '65536',
                                               'testdata/TwitterK.csv'])

    def test_parse_good_args(self):
        args = upload_file.parse_args(['--relation', 'tmp',
                                       'testdata/TwitterK.csv'])
        self.assertEquals(args.hostname, 'rest.myria.cs.washington.edu')
        self.assertEquals(args.port, 1776)
        self.assertEquals(args.program, 'adhoc')
        self.assertEquals(args.user, 'public')
        self.assertEquals(args.program, 'adhoc')
        self.assertEquals(args.relation, 'tmp')
        self.assertEquals(args.overwrite, False)

        args = upload_file.parse_args(['--relation', 'tmp',
                                       '--program', 'face',
                                       '--user', 'mom',
                                       '--overwrite',
                                       '--hostname', 'localhost',
                                       '--port', '12345',
                                       'testdata/TwitterK.csv'])
        self.assertEquals(args.hostname, 'localhost')
        self.assertEquals(args.port, 12345)
        self.assertEquals(args.user, 'mom')
        self.assertEquals(args.program, 'face')
        self.assertEquals(args.relation, 'tmp')
        self.assertEquals(args.overwrite, True)

    def test_TwitterK_csv(self):
        with HTTMock(mock_TwitterK):
            upload_file.main(['--relation', 'TwitterK',
                              '--program', 'testp',
                              '--user', 'test',
                              '--overwrite',
                              '--hostname', 'localhost',
                              '--port', '12345',
                              'testdata/TwitterK.csv'])

    def test_existing_file(self):
        with HTTMock(mock_TwitterK):
            with self.assertRaises(MyriaError):
                upload_file.main(['--relation', 'TwitterK',
                                  '--program', 'testp',
                                  '--user', 'test',
                                  '--hostname', 'localhost',
                                  '--port', '12345',
                                  'testdata/TwitterK.csv'])

    def test_TwitterKnoheader_csv(self):
        with HTTMock(mock_TwitterK):
            upload_file.main(['--relation', 'TwitterKnoheader',
                              '--program', 'testp',
                              '--user', 'test',
                              '--overwrite',
                              '--hostname', 'localhost',
                              '--port', '12345',
                              'testdata/TwitterK-noheader.csv'])

    def test_plaintext(self):
        with HTTMock(mock_TwitterK):
            upload_file.main(['--relation', 'plaintext',
                              '--program', 'testp',
                              '--user', 'test',
                              '--overwrite',
                              '--hostname', 'localhost',
                              '--port', '12345',
                              'testdata/plaintext.csv'])

    def test_float(self):
        with HTTMock(mock_TwitterK):
            upload_file.main(['--relation', 'float',
                              '--program', 'testp',
                              '--user', 'test',
                              '--overwrite',
                              '--hostname', 'localhost',
                              '--port', '12345',
                              'testdata/float.txt'])

    def test_null(self):
        with HTTMock(mock_TwitterK):
            upload_file.main(['--relation', 'nulls',
                              '--program', 'testp',
                              '--user', 'test',
                              '--overwrite',
                              '--hostname', 'localhost',
                              '--port', '12345',
                              'testdata/nulls.txt'])


def get_field(fields, name):
    (name, value, content_type) = fields[name]
    if content_type == 'application/json':
        return loads(value)
    return value


@urlmatch(netloc=r'localhost:12345')
def mock_TwitterK(url, request):
    if url.path == '/dataset':
        fields = dict(request.body.fields)
        relation_key = get_field(fields, 'relationKey')
        schema = get_field(fields, 'schema')
        overwrite = get_field(fields, 'overwrite')
        if not overwrite:
            return {'status_code': 409,
                    'content': 'That dataset already exists.'}
        assert relation_key['userName'] == 'test'
        assert relation_key['programName'] == 'testp'
        if relation_key['relationName'] == 'TwitterK':
            assert schema['columnNames'] == ['src', 'dst']
            assert schema['columnTypes'] == ['LONG_TYPE', 'LONG_TYPE']
        elif relation_key['relationName'] == 'TwitterKnoheader':
            assert schema['columnNames'] == ['column0', 'column1']
            assert schema['columnTypes'] == ['LONG_TYPE', 'LONG_TYPE']
        elif relation_key['relationName'] == 'plaintext':
            assert schema['columnNames'] == ['number', 'value']
            assert schema['columnTypes'] == ['LONG_TYPE', 'STRING_TYPE']
        elif relation_key['relationName'] == 'float':
            assert schema['columnNames'] == ['field1', 'field2']
            assert schema['columnTypes'] == ['DOUBLE_TYPE', 'DOUBLE_TYPE']
        elif relation_key['relationName'] == 'nulls':
            assert schema['columnNames'] == ['field1', 'field2', 'field3']
            assert schema['columnTypes'] == ['LONG_TYPE', 'STRING_TYPE', 'STRING_TYPE']
        else:
            assert False
        return jstr("ok")
    return None
