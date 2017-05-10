import unittest

from httmock import HTTMock
from myria.connection import MyriaConnection
from myria.test.mock import *
from myria.udf import MyriaFunction, MyriaPostgresFunction, myria_function
from raco.backends.myria.connection import FunctionTypes
from raco.myrial.parser import Parser


class TestUDF(unittest.TestCase):
    def __init__(self, args):
        with HTTMock(create_mock()):
            self.connection = MyriaConnection(hostname='localhost', port=12345)
        super(TestUDF, self).__init__(args)

    def test_get_all(self):
        with HTTMock(create_mock()):
            functions = MyriaFunction.get_all(self.connection)

            self.assertGreaterEqual(len(functions), 2)

            self.assertEqual(functions[0].language, FunctionTypes.PYTHON)
            self.assertEqual(functions[1].language, FunctionTypes.PYTHON)

            self.assertEqual(functions[0].name, UDF1_NAME)
            self.assertEqual(functions[1].name, UDF2_NAME)

            self.assertEqual(functions[0].output_type, UDF1_TYPE)
            self.assertEqual(functions[1].output_type, UDF2_TYPE)

    def test_get(self):
        with HTTMock(create_mock()):
            function = MyriaFunction.get(UDF1_NAME, self.connection)

            self.assertEqual(function.language, FunctionTypes.PYTHON)
            self.assertEqual(function.name, UDF1_NAME)
            self.assertEqual(function.output_type, UDF1_TYPE)

    def test_register(self):
        server_state = {}
        with HTTMock(create_mock(server_state)):
            f = MyriaFunction('mockudf', 'source', STRING_TYPE,
                              FunctionTypes.PYTHON, False,
                              connection=self.connection)
            f.register()

            self.assertEqual(len(server_state), 1)
            self.assertDictEqual(f.to_dict(), server_state.values()[0])
            self.assertFalse(server_state.values()[0]['isMultiValued'])
            self.assertEqual(server_state.values()[0]['outputType'],
                             'STRING_TYPE')

    def test_python_udf(self):
        name = 'pyudf'
        server_state = {}

        with HTTMock(create_mock(server_state)):
            f = MyriaPythonFunction(lambda: False, STRING_TYPE, name,
                                    multivalued=False,
                                    connection=self.connection)
            f.register()
            d = MyriaPythonFunction.from_dict(server_state[name]).to_dict()

            self.assertEqual(len(server_state), 1)
            self.assertIsNotNone(MyriaFunction.get(name, self.connection))
            self.assertIn(name, Parser.udf_functions)

            self.assertEqual(f.to_dict()['name'], d['name'])
            self.assertEqual(f.to_dict()['outputType'], d['outputType'])
            self.assertEqual(f.to_dict()['lang'], d['lang'])
            self.assertEqual(f.to_dict()['isMultiValued'], d['isMultiValued'])

    def test_postgres_udf(self):
        name = 'postgresudf'
        server_state = {}

        with HTTMock(create_mock(server_state)):
            f = MyriaPostgresFunction(name, 'source', STRING_TYPE,
                                      multivalued=False,
                                      connection=self.connection)
            f.register()
            d = MyriaPythonFunction.from_dict(server_state[name]).to_dict()

            self.assertEqual(len(server_state), 1)
            self.assertIsNotNone(MyriaFunction.get(name, self.connection))
            self.assertIn(name, Parser.udf_functions)

            self.assertEqual(f.to_dict()['name'], d['name'])
            self.assertEqual(f.to_dict()['outputType'], d['outputType'])
            self.assertEqual(f.to_dict()['isMultiValued'], d['isMultiValued'])

    def test_extension_method(self):
        server_state = {}

        with HTTMock(create_mock(server_state)):
            name = 'my_udf'

            @myria_function(name=name, output_type=STRING_TYPE,
                            connection=self.connection)
            def my_udf(t):
                return None

            self.assertEqual(len(server_state), 1)
            self.assertIsNotNone(MyriaFunction.get(name, self.connection))
            self.assertIn(name, Parser.udf_functions)

            d = MyriaPythonFunction.from_dict(server_state[name]).to_dict()
            self.assertEqual(d['name'], name)
            self.assertEqual(d['outputType'], STRING_TYPE)
