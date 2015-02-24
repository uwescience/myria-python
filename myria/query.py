import time
import requests
from . import MyriaRelation
import myria.plans

class MyriaQuery(object):
    """ Represents a Myria query """

    nonterminal_states = ['ACCEPTED', 'RUNNING']

    def __init__(self, query_id, connection=MyriaRelation.DefaultConnection,
                 timeout=60, wait_for_completion=False):
        self.query_id = query_id
        self.connection = connection
        self.timeout = timeout
        self._status = None
        self._name = None
        self._components = None
        self._qualified_name = None

        if wait_for_completion:
            self.wait_for_completion()

    @staticmethod
    def submit_plan(plan, connection=MyriaRelation.DefaultConnection,
                    timeout=60):
        """ Submit a given plan to Myria and return a new query instance """
        return MyriaQuery(connection.submit_query(plan)['queryId'],
                          connection, timeout)

    @staticmethod
    def parallel_import(relation, work, timeout=3600):
        """ Submit a new parallel ingest plan to Myria

        relation: a MyriaRelation instance that receives the imported data
        work: a sequence of (worker-id, uri) pairs assigning input to
              each worker.  Each uri may have any supported scheme (e.g.,
              file, http, hdfs) and any combination may be assigned to workers.
              For local file URIs (file://foo/bar), the file is assumed to
              be local (or locally accessible).
        """
        return MyriaQuery.submit_plan(
          myria.plans.get_parallel_import_plan(relation.schema,
            [(wid, {"dataType": "URI", "uri": uri}) for wid, uri in work],
            relation.qualified_name,
            text='Parallel Import ' + str(work)),
          relation.connection,
          timeout)

    @property
    def name(self):
        """ The name assigned to this query, if any """
        self.wait_for_completion()
        return self._name

    @property
    def qualified_name(self):
        """ A Myria-compatible dict representing the qualified name """
        self.wait_for_completion()
        return self._qualified_name

    @property
    def components(self):
        """ A list of the components [user, program, name] for the query """
        self.wait_for_completion()
        return self._components

    @property
    def status(self):
        """ The current status of the query """
        if not self._status or self._status in self.nonterminal_states:
            self._status = self.connection.get_query_status(
                             self.query_id)['status']
        return self._status

    def to_json(self):
        """ Download the JSON results of the query """
        self.wait_for_completion()
        return self.connection.download_dataset(self.qualified_name)

    def wait_for_completion(self, timeout=None):
        """ Wait up to <timeout> seconds for the query to complete """
        end = time.time() + (timeout or self.timeout)
        while self.status in self.nonterminal_states:
            if time.time() >= end:
                raise requests.Timeout()
            time.sleep(1)
        self._on_completed()

    def _on_completed(self):
        """ Load query metadata after completion """
        dataset = self.connection._wrap_get('/dataset',
                                            params={'queryId': self.query_id})
        if len(dataset):
            self._qualified_name = dataset[0]['relationKey']
            self._name = MyriaRelation._get_name(self._qualified_name)
            self._components = MyriaRelation._get_name_components(self._name)
        else:
            raise AttributeError('Unable to load query metadata '
                                 '(query status={})'.format(self.status))
