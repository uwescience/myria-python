""" Higher-level types for interacting with Myria queries """

import time
import requests
import myria.plans
from myria.relation import MyriaRelation

try:
    from pandas.core.frame import DataFrame
except ImportError:
    DataFrame = None


class MyriaQuery(object):
    """ Represents a Myria query """

    nonterminal_states = ['ACCEPTED', 'RUNNING']

    def __init__(self, query_id, connection=None,
                 timeout=60, wait_for_completion=False):
        self.query_id = query_id
        self.connection = connection or MyriaRelation.DefaultConnection
        self.timeout = timeout
        self._status = None
        self._name = None
        self._components = None
        self._qualified_name = None

        if wait_for_completion:
            self.wait_for_completion()

    @staticmethod
    def submit(query, language="MyriaL",
               connection=None,
               timeout=60,
               wait_for_completion=True):
        """ Submit a query to Myria and return a new query instance """
        connection = connection or MyriaRelation.DefaultConnection
        return MyriaQuery(
            connection.execute_program(
                query,
                language=language,
                wait_for_completion=wait_for_completion)['queryId'],
            connection, timeout)

    @staticmethod
    def submit_plan(plan, connection=None,
                    timeout=60):
        """ Submit a given plan to Myria and return a new query instance """
        connection = connection or MyriaRelation.DefaultConnection
        return MyriaQuery(connection.submit_query(plan)['queryId'],
                          connection, timeout)

    @staticmethod
    def parallel_import(relation, work, timeout=3600,
                        scan_type=None, scan_parameters=None,
                        insert_type=None, insert_parameters=None):
        """ Submit a new parallel ingest plan to Myria

        relation: a MyriaRelation instance that receives the imported data
        work: a sequence of (worker-id, uri) pairs assigning input to
              each worker.  Each uri may have any supported scheme (e.g.,
              file, http, hdfs) and any combination may be assigned to workers.
              For local file URIs (file://foo/bar), the file is assumed to
              be local (or locally accessible).
        scan_type: Reader parameters, e.g., {'readerType': 'CSV', "skip": 1}.
                Schema is inserted into this.
        scan_parameters: Additional options to the TupleSource operator.
        """
        return MyriaQuery.submit_plan(
            myria.plans.get_parallel_import_plan(
                relation.schema,
                [(wid, {"dataType": "URI", "uri": uri}) for wid, uri in work],
                relation.qualified_name,
                text='Parallel Import ' + str(work),
                scan_type=scan_type,
                scan_parameters=scan_parameters,
                insert_type=insert_type,
                insert_parameters=insert_parameters),
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

    def kill(self):
        """ Kill this query """
        self.connection.kill_query(self.query_id)
        self._status = None

    def to_dict(self, limit=None):
        """ Download the JSON results of the query """
        self.wait_for_completion()
        return self.connection.download_dataset(self.qualified_name, limit) \
            if self.qualified_name else None

    def to_dataframe(self, index=None, limit=None):
        """ Convert the query result to a Pandas DataFrame """
        if not DataFrame:
            raise ImportError('Must execute `pip install pandas` to generate '
                              'Pandas DataFrames')
        else:
            values = self.to_dict(limit)
            return DataFrame.from_records(values, index=index) \
                if values else None

    def _repr_html_(self, limit=None):
        """ Generate a representation of this query as HTML """
        if self.status in self.nonterminal_states:
            return '<{}, status={}>'.format(
                self.__class__.__name__, self.status)
        else:
            limit = limit or MyriaRelation.DisplayLimit
            dataframe = self.to_dataframe(limit=limit)
            footer = '<p>(First {} tuples shown)</p>'.format(limit) \
                if limit and len(dataframe) > limit else ''
            return dataframe.to_html() + footer

    def wait_for_completion(self, timeout=None):
        """ Wait up to <timeout> seconds for the query to complete """
        end = time.time() + (timeout or self.timeout)
        while self.status in self.nonterminal_states:
            if time.time() >= end:
                raise requests.Timeout()
            time.sleep(1)
        self._on_completed()
        return self

    def _on_completed(self):
        """ Load query metadata after query completion """
        dataset = self.connection._wrap_get('/dataset',
                                            params={'queryId': self.query_id})
        if len(dataset):
            self._qualified_name = dataset[0]['relationKey']
            self._name = MyriaRelation._get_name(self._qualified_name)
            self._components = MyriaRelation._get_name_components(self._name)
