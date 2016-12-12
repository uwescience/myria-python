import unittest
import json

<<<<<<< HEAD
from httmock import HTTMock
from myria import MyriaSchema
=======
from raco.algebra import CrossProduct, Join, ProjectingJoin, Apply, Select
from raco.expression import UnnamedAttributeRef, TAUTOLOGY, COUNTALL, COUNT, \
    PythonUDF
from raco.types import STRING_TYPE, BOOLEAN_TYPE, LONG_TYPE

>>>>>>> Support for Python UDFs in fluent API, extension methods
from myria.connection import MyriaConnection
from myria.fluent import myria_function
from myria.relation import MyriaRelation
<<<<<<< HEAD
from myria.test.mock import create_mock, FULL_NAME, FULL_NAME2, UDF1_ARITY, \
    UDF1_TYPE, SCHEMA
from myria.udf import myria_function
from raco.algebra import CrossProduct, Join, ProjectingJoin, Apply, Select
from raco.expression import UnnamedAttributeRef, TAUTOLOGY, COUNTALL, COUNT, \
    PYUDF
from raco.types import STRING_TYPE, BOOLEAN_TYPE
=======
from myria.udf import MyriaPythonFunction

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

UDF1_NAME, UDF2_NAME = 'udf1', 'udf2'
UDF1_TYPE, UDF2_TYPE = LONG_TYPE, STRING_TYPE
UDF1_ARITY, UDF2_ARITY = 1, 2


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

    elif url.path == '/function':
        return {
            'status_code': 200,
            'content': [
                MyriaPythonFunction(UDF1_NAME, UDF1_TYPE,
                                    lambda i: 0, UDF1_ARITY).to_dict(),
                MyriaPythonFunction(UDF2_NAME, UDF2_TYPE,
                                    lambda i: 0, UDF2_ARITY).to_dict()]}

    elif url.path == '/function/register':
        return {'status_code': 200, 'content': '{}'}

    return None
>>>>>>> Support for Python UDFs in fluent API, extension methods


class TestFluent(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(create_mock()):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        super(TestFluent, self).__init__(args)

    def test_scan(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            json = relation._sink().to_json()
            optype = json['plan']['fragments'][0]['operators'][0]['opType']
            name = json['plan']['fragments'][0]['operators'][0]['opName']
            self.assertTrue('Scan' in optype)
            self.assertTrue(relation.name in name)

    def test_load(self):
        state = {}
        with HTTMock(create_mock(state)):
            url = 'file:///foo.bar'
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            relation.load(url, schema=MyriaSchema(SCHEMA))
            plan = relation._sink().to_json()
            text = json.dumps(plan['plan']['fragments'][1]['operators'][0])
            self.assertTrue('FileScan' in text)
            self.assertTrue('TupleSource' in text)
            self.assertTrue(url in text)

            relation.execute()
            plan = state['query']
            text = json.dumps(plan['plan']['fragments'][0]['operators'][0])
            self.assertTrue('FileScan' in text)
            self.assertTrue('TupleSource' in text)
            self.assertTrue(url in text)

    def test_static_load(self):
        state = {}
        with HTTMock(create_mock(state)):
            url = 'file:///foo.bar'
            MyriaRelation.load(FULL_NAME, url, MyriaSchema(SCHEMA),
                               connection=self.connection)
            plan = state['query']
            text = json.dumps(plan['plan']['fragments'][1]['operators'][0])
            self.assertTrue('FileScan' in text)
            self.assertTrue('TupleSource' in text)
            self.assertTrue(url in text)

    def test_project_positional_expression(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(lambda t: t.column + 12345678)
            json = projected._sink().to_json()
            self.assertTrue('12345678' in str(json))

    def test_project_positional_string(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select("column")
            sunk = projected._sink()
            sunk2 = sunk
            json = sunk2.to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_project_named_expression(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo=lambda t: t.column + 12345678)
            json = projected._sink().to_json()
            self.assertTrue('foo' in str(json))
            self.assertTrue('12345678' in str(json))

    def test_project_named_string(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo='column')
            json = projected._sink().to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue('foo' in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_project_multiple(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            projected = relation.select(foo='column', bar='column')
            json = projected._sink().to_json()
            self.assertTrue("outputName" in str(json))
            self.assertTrue('foo' in str(json))
            self.assertTrue('bar' in str(json))
            self.assertTrue("column" in str(json))
            self.assertFalse("column2" in str(json))

    def test_select_expression(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            selected = relation.where(lambda t: t.column < 123456)
            json = selected._sink().to_json()
            self.assertTrue("LT" in str(json))
            self.assertTrue('123456' in str(json))
            self.assertTrue("column" in str(json))

    def test_select_string(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            selected = relation.where("12345 + 67890")
            json = selected._sink().to_json()
            self.assertTrue("PLUS" in str(json))
            self.assertTrue('12345' in str(json))
            self.assertTrue("67890" in str(json))

    def test_product(self):
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
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
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count()

            self.assertListEqual(count.query.grouping_list, [])
            self.assertListEqual(count.query.aggregate_list, [COUNTALL()])
            self.assertIsNotNone(count._sink().to_json())

    def test_count_attribute(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count('column2')

            self.assertListEqual(count.query.grouping_list, [])
            self.assertListEqual(count.query.aggregate_list,
                                 [COUNT(UnnamedAttributeRef(1))])
            self.assertIsNotNone(count._sink().to_json())

    def test_count_groups(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            count = relation.count('column2', groups='column')

            self.assertListEqual(count.query.grouping_list,
                                 [UnnamedAttributeRef(0)])
            self.assertListEqual(count.query.aggregate_list,
                                 [COUNT(UnnamedAttributeRef(1))])
            self.assertIsNotNone(count._sink().to_json())

    def test_python_registered_udf(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            udf1 = id
            udf = relation.select(lambda t: udf1(t[0]))

            applys = filter(lambda op: isinstance(op, Apply),
                            udf.query.walk())
            self.assertEqual(len(applys), 1)

            _apply = applys[0] if applys else None
            self.assertIsNotNone(_apply)
            self.assertEqual(len(_apply.emitters), 1)

            pyudf = _apply.emitters[0][1] if apply else None
            self.assertIsInstance(pyudf, PYUDF)
            self.assertEqual(pyudf.typ, UDF1_TYPE)
            self.assertTrue(pyudf.arguments, UDF1_ARITY)

    def test_python_udf(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            udf = relation.select(lambda t: eval("5 < 10"))

            _apply = next(iter(filter(lambda op: isinstance(op, Apply),
                                      udf.query.walk())), None)
            self.assertIsNotNone(_apply)
            self.assertEqual(len(_apply.emitters), 1)

            pyudf = _apply.emitters[0][1] if apply else None
            self.assertIsInstance(pyudf, PYUDF)
            self.assertEqual(pyudf.typ, STRING_TYPE)
            self.assertTrue(pyudf.arguments, 2)
            self.assertEqual([n.get_val() for n in pyudf.arguments],
                             SCHEMA['columnNames'])

    def test_python_udf_predicate(self):
        with HTTMock(create_mock()):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)
            udf = relation.where(lambda t: eval("t[0] < 10"))

            select = next(iter(filter(lambda op: isinstance(op, Select),
                                      udf.query.walk())), None)
            self.assertIsNotNone(select)

            pyudf = select.condition
            self.assertIsInstance(pyudf, PYUDF)
            self.assertEqual(pyudf.typ, BOOLEAN_TYPE)
            self.assertTrue(pyudf.arguments, 2)
            self.assertEqual([n.get_val() for n in pyudf.arguments],
                             SCHEMA['columnNames'])

    def test_extension_method(self):
        server_state = {}
        with HTTMock(create_mock(server_state)):

            relation = MyriaRelation(FULL_NAME, connection=self.connection)

            @myria_function(name='my_udf', output_type=BOOLEAN_TYPE)
            def extension(column1, column2):
                return str(column1) == str(column2)

            udf = relation.my_udf()

            _apply = next(iter(filter(lambda op: isinstance(op, Apply),
                                      udf.query.walk())), None)
            self.assertIsNotNone(_apply)
            self.assertEqual(len(_apply.emitters), 1)

            pyudf = _apply.emitters[0][1] if apply else None
            self.assertIsInstance(pyudf, PYUDF)
            self.assertEqual(pyudf.typ, BOOLEAN_TYPE)
            self.assertTrue(pyudf.arguments, 2)
            self.assertEqual([n.get_val() for n in pyudf.arguments],
                             SCHEMA['columnNames'])

            self.assertEqual(len(server_state), 1)
            self.assertFalse(server_state.values()[0]['isMultiValued'])
            self.assertEqual(server_state.values()[0]['outputType'],
                             'BOOLEAN_TYPE')

    def test_multivalued_extension_method(self):
        server_state = {}
        with HTTMock(create_mock(server_state)):
            relation = MyriaRelation(FULL_NAME, connection=self.connection)

            @myria_function(name='my_udf', output_type=BOOLEAN_TYPE,
                            multivalued=True)
            def extension(column1, column2):
                return [str(column1) == str(column2),
                        str(column1) == str(column2)]

            udf = relation.my_udf()

            _apply = next(iter(filter(lambda op: isinstance(op, Apply),
                                      udf.query.walk())), None)
            self.assertIsNotNone(_apply)
            self.assertEqual(len(_apply.emitters), 1)

            pyudf = _apply.emitters[0][1] if apply else None
            self.assertIsInstance(pyudf, PYUDF)
            self.assertEqual(pyudf.typ, BOOLEAN_TYPE)
            self.assertTrue(pyudf.arguments, 2)
            self.assertEqual([n.get_val() for n in pyudf.arguments],
                             SCHEMA['columnNames'])

            self.assertEqual(len(server_state), 1)
            self.assertTrue(server_state.values()[0]['isMultiValued'])
            self.assertEqual(server_state.values()[0]['outputType'],
                             'BOOLEAN_TYPE')

