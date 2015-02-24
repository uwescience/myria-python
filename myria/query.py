import time
import requests
from . import MyriaRelation
import myria.plans

class MyriaQuery(object):
  nonterminal_states = ['ACCEPTED', 'RUNNING']

  def __init__(self, id, connection=MyriaRelation.DefaultConnection, timeout=60, *args, **kwargs):
    self.id = id
    self.connection = connection
    self.timeout = timeout
    self._status = None

    if kwargs.get('waitForCompletion', False): self.waitForCompletion()

  @staticmethod
  def submit_plan(plan, connection=MyriaRelation.DefaultConnection, timeout=60):
    return MyriaQuery(connection.submit_query(plan)['queryId'], connection)

  @staticmethod
  def parallel_import(relation, uris, timeout=3600):
    return MyriaQuery.submit_plan(
      myria.plans.get_parallel_import_plan(relation.schema, 
        map(lambda (id, uri): (id, { "dataType": "URI", "uri": uri }), uris), 
        relation.qualified_name, 
        text='Parallel Import ' + str(uris)),
      relation.connection,
      timeout)

  @property
  def name(self):
    self.waitForCompletion()
    return self._name
  
  @property
  def qualified_name(self):
    self.waitForCompletion()
    return self._qualified_name

  @property
  def components(self):
    self.waitForCompletion()
    return self._components

  @property
  def status(self):
    if not self._status or self._status in self.nonterminal_states:
      self._status = self.connection.get_query_status(self.id)['status']
    return self._status

  def toJson(self):
    self.waitForCompletion()
    return self.connection.download_dataset(self.qualified_name)

  def waitForCompletion(self, timeout=None):
    end = time.time() + (timeout or self.timeout)
    while self.status in self.nonterminal_states:
      if time.time() >= end: 
        raise requests.Timeout()
      time.sleep(1)
    self.__on_completed()

  def __on_completed(self):
    dataset = self.connection._wrap_get('/dataset', params={'queryId': self.id})
    if len(dataset):
      self._qualified_name = dataset[0]['relationKey']
      self._name = MyriaRelation._get_name(self._qualified_name)
      self._components = MyriaRelation._get_name_components(self._name)
    else:
      raise AttributeError('Unable to load query metadata (query status={})'.format(self.status))
