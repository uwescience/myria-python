from httmock import urlmatch, HTTMock
from datetime import datetime
import unittest

from raco.algebra import CrossProduct, Join, ProjectingJoin
from raco.expression import UnnamedAttributeRef, TAUTOLOGY, COUNTALL, COUNT

from myria.connection import MyriaConnection
from myria.relation import MyriaRelation

RELATION_NAME = 'relation'
FULL_NAME = 'public:adhoc:' + RELATION_NAME
QUALIFIED_NAME = {'userName': 'public',
                  'programName': 'adhoc',
                  'relationName': RELATION_NAME}
NAME_COMPONENTS = ['public', 'adhoc', RELATION_NAME]
SCHEMA = {'columnNames': ['column', 'column2'],
          'columnTypes': ['INT_TYPE', 'INT_TYPE']}
CREATED_DATE = datetime(1900, 1, 2, 3, 4)
TUPLES = [[1, 9], [2, 8], [3, 7], [4, 6], [5, 5]]
TOTAL_TUPLES = len(TUPLES)

RELATION_NAME2 = 'relation2'
FULL_NAME2 = 'public:adhoc:' + RELATION_NAME2
QUALIFIED_NAME2 = {'userName': 'public',
                   'programName': 'adhoc',
                   'relationName': RELATION_NAME2}
NAME_COMPONENTS2 = ['public', 'adhoc', RELATION_NAME2]
SCHEMA2 = {'columnNames': ['column3', 'column4'],
           'columnTypes': ['INT_TYPE', 'INT_TYPE']}
TUPLES2 = [[1, 9], [2, 8], [3, 7], [4, 6], [5, 5]]
TOTAL_TUPLES2 = len(TUPLES2)


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
    elif url.path == get_uri(RELATION_NAME2):
        body = {'numTuples': TOTAL_TUPLES2,
                'schema': SCHEMA2,
                'created': str(CREATED_DATE)}
        return {'status_code': 200, 'content': body}

    # Relation download
    if url.path == get_uri(RELATION_NAME) + '/data':
        body = str(TUPLES)
        return {'status_code': 200, 'content': body}
    elif url.path == get_uri(RELATION_NAME2) + '/data':
        body = str(TUPLES2)
        return {'status_code': 200, 'content': body}

    # Relation not found in database
    elif get_uri('NOTFOUND') in url.path:
        return {'status_code': 404}

    return None


class TestFluent(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(local_mock):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        super(TestFluent, self).__init__(args)

    def test_scan(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            json = relation._sink().to_json()
            optype = json['plan']['fragments'][0]['operators'][0]['opType']
            name = json['plan']['fragments'][0]['operators'][0]['opName']
            self.assertTrue('Scan' in optype)
            self.assertTrue(relation.name in name)

    def test_project_positional_expression(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(lambda t: t.column + 12345678)
            json = projected._sink().to_json()
            self.assertTrue('12345678' in str(json))

    def test_project_positional_string(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select("column")
            sunk = projected._sink()
            sunk2 = sunk
            json = sunk2.to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_project_named_expression(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo=lambda t: t.column + 12345678)
            json = projected._sink().to_json()
            self.assertTrue('foo' in str(json))
            self.assertTrue('12345678' in str(json))

    def test_project_named_string(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo='column')
            json = projected._sink().to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue('foo' in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_project_multiple(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo='column', bar='column')
            json = projected._sink().to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue('foo' in str(json))
            self.assertTrue('bar' in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_select_expression(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            selected = relation.where(lambda t: t.column < 123456)
            json = selected._sink().to_json()
            self.assertTrue("LT" in str(json))
            self.assertTrue('123456' in str(json))
            self.assertTrue("column" in str(json))

    def test_select_string(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            selected = relation.where("12345 + 67890")
            json = selected._sink().to_json()
            self.assertTrue("PLUS" in str(json))
            self.assertTrue('12345' in str(json))
            self.assertTrue("67890" in str(json))

    def test_product(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            product = left.join(right)

            join = filter(lambda op: isinstance(op, CrossProduct),
                          product.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(product._sink().to_json())

    def test_join(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right)

            join = filter(
                lambda o: isinstance(o, Join) or isinstance(o, CrossProduct),
                joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(joined._sink().to_json())

    def test_join_predicate(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right, lambda l, r: l.column == r.column3)

            join = filter(lambda op: isinstance(op, Join),
                          joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(join[0].condition)
            self.assertIsNotNone(join[0].condition)
            self.assertNotEqual(join[0].condition, TAUTOLOGY)
            self.assertIsNotNone(joined._sink().to_json())

    def test_join_positional_attribute(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right,
                               lambda l, r: l.column == r.column3,
                               projection=[0])

            join = filter(lambda op: isinstance(op, ProjectingJoin),
                          joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(join[0].output_columns)
            self.assertListEqual(join[0].output_columns,
                                 [UnnamedAttributeRef(0)])
            self.assertIsNotNone(joined._sink().to_json())

    def test_join_named_attribute(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right,
                               lambda l, r: l.column == r.column3,
                               projection=['column2'])

            join = filter(lambda op: isinstance(op, ProjectingJoin),
                          joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(join[0].output_columns)
            self.assertListEqual(join[0].output_columns,
                                 [UnnamedAttributeRef(1)])
            self.assertIsNotNone(joined._sink().to_json())

    def test_join_dotted_attribute(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right,
                               lambda l, r: l.column == r.column3,
                               projection=['right.column3'],
                               aliases=['left', 'right'])

            join = filter(lambda op: isinstance(op, ProjectingJoin),
                          joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(join[0].output_columns)
            self.assertListEqual(join[0].output_columns,
                                 [UnnamedAttributeRef(2)])
            self.assertIsNotNone(joined._sink().to_json())

    def test_join_lambda_attribute(self):
        with HTTMock(local_mock):
            left = MyriaRelation(FULL_NAME, connection=self.connection)
            right = MyriaRelation(FULL_NAME2, connection=self.connection)
            joined = left.join(right,
                               lambda l, r: l.column == r.column3,
                               projection=[lambda l, r: l.column2])

            join = filter(lambda op: isinstance(op, ProjectingJoin),
                          joined.query.walk())
            self.assertTrue(join)
            self.assertEqual(join[0].left, left.query)
            self.assertEqual(join[0].right, right.query)
            self.assertIsNotNone(join[0].output_columns)
            self.assertListEqual(join[0].output_columns,
                                 [UnnamedAttributeRef(1)])
            self.assertIsNotNone(joined._sink().to_json())

    def test_count(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count()

            self.assertListEqual(count.query.grouping_list, [])
            self.assertListEqual(count.query.aggregate_list, [COUNTALL()])
            self.assertIsNotNone(count._sink().to_json())

    def test_count_attribute(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count('column2')

            self.assertListEqual(count.query.grouping_list, [])
            self.assertListEqual(count.query.aggregate_list,
                                 [COUNT(UnnamedAttributeRef(1))])
            self.assertIsNotNone(count._sink().to_json())

    def test_count_groups(self):
        with HTTMock(local_mock):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count('column2', groups='column')

            self.assertListEqual(count.query.grouping_list,
                                 [UnnamedAttributeRef(0)])
            self.assertListEqual(count.query.aggregate_list,
                                 [COUNT(UnnamedAttributeRef(1))])
            self.assertIsNotNone(count._sink().to_json())
