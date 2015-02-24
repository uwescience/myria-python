from dateutil.parser import parse
from itertools import izip
from myria import MyriaConnection, MyriaError
from schema import MyriaSchema

class MyriaRelation(object):
  DefaultConnection = MyriaConnection(hostname='localhost', port=8753)

  # MyriaRelation({'userName': 'public', 'programName': 'adhoc', 'relationName': 'relation'})
  # or
  # MyriaRelation('public:adhoc:relation')
  # or
  # MyriaRelation('relation') # Defaults are 'public' and 'adhoc'
  def __init__(self, relation, connection=DefaultConnection, *args, **kwargs):
    self.name = relation if isinstance(relation, basestring) else relation.name
    self.components = self._get_name_components(self.name)
    self.connection = connection
    self.qualified_name = self._get_qualified_name(self.components)
    self._schema = kwargs['schema'] if 'schema' in kwargs else None

    # Should probably expose a better way of creating "new" relations
    if not self._schema is None and self._has_metadata:
      raise MyriaError('New relation specified (schema != None), but it already exists on the server.')

  def toJson(self):
    return self.connection.download_dataset(self.qualified_name)

  @property 
  def schema(self):
    if self._schema is None:
      self._schema = MyriaSchema(json=self._metadata['schema'])
    return self._schema

  @property
  def createdDate(self):
    return parse(self._metadata['created'])

  def __len__(self):
    return int(self._metadata['numTuples'])

  @property
  def _metadata(self):
    if 'metadata' not in self.__dict__:
      self.metadata = self.connection.dataset(self.qualified_name)
    return self.metadata

  @property 
  def _has_metadata(self):
    try:
      return bool(self._metadata)
    except MyriaError:
      return False

  @staticmethod
  def _get_name(qualified_name):
    return ':'.join([qualified_name['userName'], 
                     qualified_name['programName'], 
                     qualified_name['relationName']])

  @staticmethod
  def _get_name_components(name):
    components = name.split(':')
    default_components = ['public', 'adhoc'][:max(3 - len(components), 0)]
    return default_components + components[:3]

  @staticmethod
  def _get_qualified_name(name_or_components):
    if isinstance(name_or_components, basestring):
      return MyriaRelation._get_qualified_name(MyriaRelation._get_name_components(name_or_components))
    else:
      return dict(izip(('userName', 'programName', 'relationName'), name_or_components[:3]))
