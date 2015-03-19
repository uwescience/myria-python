""" Higher-level types for interacting with Myria schema """

SCHEMA_TYPES = ['INT_TYPE', 'FLOAT_TYPE', 'DOUBLE_TYPE', 'BOOLEAN_TYPE',
                'STRING_TYPE', 'LONG_TYPE', 'DATETIME_TYPE']


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

    def to_json(self):
        ''' Convert this schema instance to JSON '''
        return {'columnNames': self.names,
                'columnTypes': self.types}
