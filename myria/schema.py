""" Higher-level types for interacting with Myria schema """

SCHEMA_TYPES = ['INT_TYPE', 'FLOAT_TYPE', 'DOUBLE_TYPE', 'BOOLEAN_TYPE',
                'STRING_TYPE', 'LONG_TYPE', 'DATETIME_TYPE', 'BYTES_TYPE']


class MyriaSchema(object):
    """ Represents a schema for a Myria relation """

    def __init__(self, json):
        if len(json['columnNames']) == 0:
            raise ValueError('Schema must have at least one attribute.')
        if len(json['columnNames']) != len(json['columnTypes']):
            raise ValueError('Schema must have the same number of attributes '
                             'and types.')
        if any(value not in SCHEMA_TYPES for value in json['columnTypes']):
            raise ValueError('One or more of the following types are '
                             'invalid: ' + ', '.join(json['columnTypes']))

        self.json = json
        self.names = json['columnNames']
        self.types = json['columnTypes']

    def __len__(self):
        return len(self.names)

    def __eq__(self, other):
        return isinstance(other, MyriaSchema) and self.json == other.json

    def __ne__(self, other):
        return not self == other

    def to_dict(self):
        """ Convert this schema instance to JSON """
        return {'columnNames': self.names,
                'columnTypes': self.types}
