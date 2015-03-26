from httmock import urlmatch, HTTMock
from datetime import datetime
import unittest
from myria.connection import MyriaConnection
from myria.relation import MyriaRelation
from myria.schema import MyriaSchema


RELATION_NAME = 'relation'
FULL_NAME = 'public:adhoc:' + RELATION_NAME
QUALIFIED_NAME = {'userName': 'public',
                  'programName': 'adhoc',
                  'relationName': RELATION_NAME}
NAME_COMPONENTS = ['public', 'adhoc', RELATION_NAME]
SCHEMA = {'columnNames': ['column'], 'columnTypes': ['INT_TYPE']}
CREATED_DATE = datetime(1900, 1, 2, 3, 4)
TUPLES = [[1], [2], [3], [4], [5]]
TOTAL_TUPLES = len(TUPLES)


def get_uri(name):
    return '/dataset/user-{}/program-{}/relation-{}'.format(
        'public', 'adhoc', name)


@urlmatch(netloc=r'localhost:12345')
def local_mock(url, request):
    # Relation metadata
    if url.path == get_uri(RELATION_NAME):
        body = {'numTuples': TOTAL_TUPLES,
                'schema': SCHEMA,
                'created': str(CREATED_DATE)}
        return {'status_code': 200, 'content': body}

    # Relation download
    if url.path == get_uri(RELATION_NAME) + '/data':
        body = str(TUPLES)
        return {'status_code': 200, 'content': body}

    # Relation not found in database
    elif get_uri('NOTFOUND') in url.path:
        return {'status_code': 404}

    return None


class TestRelation(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        super(TestRelation, self).__init__(args)

    def test_connection(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            self.assertEquals(relation.connection, self.connection)

            relation = MyriaRelation(FULL_NAME)
            self.assertEquals(relation.connection,
                              MyriaRelation.DefaultConnection)

    def test_name(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            self.assertEquals(relation.name, FULL_NAME)
            self.assertDictEqual(relation.qualified_name, QUALIFIED_NAME)
            self.assertListEqual(relation.components, NAME_COMPONENTS)
            self.assertEquals(relation._get_name(relation.qualified_name),
                              FULL_NAME)

    def test_unpersisted_relation(self):
        with HTTMock(local_mock):
            self.assertFalse(MyriaRelation(
                'public:adhoc:NOTFOUND',
                connection=self.connection).is_persisted)

    def test_persisted_relation(self):
        with HTTMock(local_mock):
            self.assertTrue(MyriaRelation(
                FULL_NAME, connection=self.connection).is_persisted)

    def test_persisted_with_schema(self):
        with HTTMock(local_mock):
            self.assertIsInstance(MyriaRelation(FULL_NAME,
                                                connection=self.connection,
                                                schema=MyriaSchema(SCHEMA)),
                                  MyriaRelation)

            different_name = {'columnNames': ['foo'],
                              'columnTypes': ['INT_TYPE']}
            self.assertRaises(ValueError,
                              MyriaRelation,
                              FULL_NAME,
                              connection=self.connection,
                              schema=MyriaSchema(different_name))

            different_type = {'columnNames': ['column'],
                              'columnTypes': ['STRING_TYPE']}
            self.assertRaises(ValueError,
                              MyriaRelation,
                              FULL_NAME,
                              connection=self.connection,
                              schema=MyriaSchema(different_type))

    def test_persisted_schema(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            self.assertDictEqual(relation.schema.to_json(), SCHEMA)

    def test_created_date(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)

            self.assertEquals(relation.created_date, CREATED_DATE)

    def test_len(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)

            self.assertEquals(len(relation), TOTAL_TUPLES)

    def test_json_download(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)

            self.assertListEqual(relation.to_json(), TUPLES)

    def test_unpersisted_json_download(self):
        with HTTMock(local_mock):
            relation = MyriaRelation('public:adhoc:NOTFOUND',
                                     connection=self.connection)

            self.assertEquals(relation.to_json(), [])
