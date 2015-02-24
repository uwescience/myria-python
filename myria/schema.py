class MyriaSchema(object):
  def __init__(self, json):
    self.json = json
    self.names = json['columnNames']
    self.types = json['columnTypes']

  def toJson(self):
    return {'columnNames': map(lambda s: s.encode('ascii'), self.names), 
            'columnTypes': map(lambda s: s.encode('ascii'), self.types)}
