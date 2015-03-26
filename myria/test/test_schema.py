import unittest
from myria.schema import MyriaSchema


class TestSchema(unittest.TestCase):
    def test_empty(self):
        empty_schema = {'columnNames': [], 'columnTypes': []}
        self.assertRaises(ValueError, MyriaSchema, empty_schema)

    def test_argument_lengths(self):
        attribute_mismatch = {'columnNames': ['column1'], 'columnTypes': []}
        self.assertRaises(ValueError, MyriaSchema, attribute_mismatch)

        type_mismatch = {'columnNames': [], 'columnTypes': ['INT_TYPE']}
        self.assertRaises(ValueError, MyriaSchema, type_mismatch)

    def test_invalid_types(self):
        invalid_type = {'columnNames': ['foo'], 'columnTypes': ['FOO_TYPE']}
        self.assertRaises(ValueError, MyriaSchema, invalid_type)

    def test_names(self):
        names = ['column1']
        schema = {'columnNames': names, 'columnTypes': ['INT_TYPE']}
        self.assertListEqual(names, MyriaSchema(schema).names)

    def test_types(self):
        types = ['INT_TYPE']
        schema = {'columnNames': ['foo'], 'columnTypes': types}
        self.assertListEqual(types, MyriaSchema(schema).types)

    def test_json(self):
        schema = {'columnNames': ['foo'], 'columnTypes': ['INT_TYPE']}
        self.assertDictEqual(schema, MyriaSchema(schema).to_json())
