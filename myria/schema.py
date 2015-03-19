""" Higher-level types for interacting with Myria schema """


class MyriaSchema(object):
    """ Represents a schema for a Myria relation """

    def __init__(self, json):
        self.json = json
        self.names = json['columnNames']
        self.types = json['columnTypes']

    def to_json(self):
        ''' Convert this schema instance to JSON '''
        return {'columnNames': self.names,
                'columnTypes': self.types}
