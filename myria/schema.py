class MyriaSchema(object):
    """ Represents a schema for a Myria relation """

    def __init__(self, json):
        self.json = json
        self.names = json['columnNames']
        self.types = json['columnTypes']

    def to_json(self):
        ''' Convert this schema instance to JSON '''
        return {'columnNames': [n.encode('ascii') for n in self.names],
                'columnTypes': [t.encode('ascii') for t in self.types]}
