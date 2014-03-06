import base64
import ConfigParser
import json
import csv
from time import sleep
import logging

import requests

from .errors import MyriaError

__all__ = ['MyriaConnection']

# String constants used in forming requests
JSON = 'application/json'
CSV = 'text/plain'
GET = 'GET'
PUT = 'PUT'
POST = 'POST'

# Enable or configure logging
logging.basicConfig(level=logging.WARN)


class MyriaConnection(object):
    """Contains a connection the Myria REST server."""

    _DEFAULT_HEADERS = {
        'Accept': JSON,
        'Content-Type': JSON
    }

    @staticmethod
    def _parse_deployment(deployment):
        "Extract the REST server hostname and port from a deployment.cfg file"
        if deployment is None:
            return None
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        config.readfp(deployment)
        master = config.get('master', '0')
        hostname = master[:master.index(':')]
        port = int(config.get('deployment', 'rest_port'))
        return (hostname, port)

    def __init__(self,
                 deployment=None,
                 hostname=None,
                 port=None,
                 timeout=None):
        """Initializes a connection to the Myria REST server.

        Args:
            deployment: An open file (or other buffer) containing a
                deployment.cfg file in Myria's format. This file will be parsed
                to determine the REST server hostname and port.
            hostname: The hostname of the REST server. May be overwritten if
                deployment is provided.
            port: The port of the REST server. May be overwritten if deployment
                is provided.
            timeout: The timeout for the connection to myria.
        """
        # Parse the deployment file and, if present, override the hostname and
        # port with any provided values from deployment.
        rest_config = self._parse_deployment(deployment)
        if rest_config is not None:
            hostname = hostname or rest_config[0]
            port = port or rest_config[1]

        self._url_start = 'http://{}:{}'.format(hostname, port)
        self._session = requests.Session()
        self._session.headers.update(self._DEFAULT_HEADERS)

    def _finish_async_request(self, method, url, body=None, accept=JSON):
        headers = {
            'Accept': accept
        }
        try:
            while True:
                if '://' not in url:
                    url = self._url_start + url
                logging.info("Finish async request to {}. Headers: {}".format(
                    url, headers))
                r = self._session.request(method, url, headers=headers,
                                          data=body)
                if r.status_code in [200, 201]:
                    if accept == JSON:
                        return r.json()
                    else:
                        return r.text
                elif r.status_code in [202]:
                    # Get the new URL to poll, etc.
                    url = r.headers['Location']
                    method = GET
                    body = None
                    # Read and ignore the body
                    # response.read()
                    # Sleep 100 ms before re-issuing the request
                    sleep(0.1)
                else:
                    raise MyriaError('Error %d: %s'
                                     % (r.status_code, r.text))
        except Exception as e:
            if isinstance(e, MyriaError):
                raise
            raise MyriaError(e)

    def _make_request(self, method, url, body=None, params=None,
                      accept=JSON, get_request=False):
        headers = {
            'Accept': accept
        }
        try:
            if '://' not in url:
                url = self._url_start + url
            r = self._session.request(method, url, headers=headers,
                                      data=body, params=params, stream=True)
            logging.info("Make myria request to {}. Headers: {}".format(
                         r.url, headers))
            if r.status_code in [200, 201, 202]:
                if get_request:
                    return r
                if accept == JSON:
                    try:
                        return r.json()
                    except ValueError, e:
                        raise MyriaError(
                            'Error %d: %s' % (r.status_code, r.text))
                else:
                    return r.iter_lines()
            else:
                raise MyriaError('Error %d: %s'
                                 % (r.status_code, r.text))
        except Exception as e:
            if isinstance(e, MyriaError):
                raise
            raise MyriaError(e)

    def _wrap_get(self, selector, params=None, status=None, accepted=None):
        if status is None:
            status = [200]
        if accepted is None:
            accepted = []

        if '://' not in selector:
            selector = self._url_start + selector
        r = self._session.get(selector)
        if r.status_code in status:
            return r.json()
        elif r.status_code in accepted:
            return self._wrap_get(selector, params=params, status=status,
                                  accepted=accepted)
        else:
            raise MyriaError(r)

    def _wrap_post(self, selector, data=None, params=None, status=None,
                   accepted=None):
        if status is None:
            status = [201, 202]
            if accepted is None:
                accepted = [202]
        else:
            if accepted is None:
                accepted = []

        if '://' not in selector:
            selector = self._url_start + selector
        r = self._session.post(selector, data=data, params=params)
        if r.status_code in status:
            if r.headers['Location']:
                return self._wrap_get(r.headers['Location'], status=status,
                                      accepted=accepted)
            return r.json()
        else:
            raise MyriaError(r)

    def workers(self):
        """Return a dictionary of the workers"""
        return self._wrap_get('/workers')

    def workers_alive(self):
        """Return a list of the workers that are alive"""
        return self._wrap_get('/workers/alive')

    def worker(self, worker_id):
        """Return information about the specified worker"""
        return self._wrap_get('/workers/worker-{}'.format(worker_id))

    def datasets(self):
        """Return a list of the datasets that exist"""
        return self._wrap_get('/dataset')

    def dataset(self, relation_key):
        """Return information about the specified relation"""
        return self._wrap_get('/dataset/user-{}/program-{}/relation-{}'.format(
            relation_key['userName'],
            relation_key['programName'],
            relation_key['relationName']))

    def download_dataset(self, relation_key):
        """Download the data in the dataset as json"""
        return self._wrap_get('/dataset/user-{}/program-{}/relation-{}'.format(
                              relation_key['userName'],
                              relation_key['programName'],
                              relation_key['relationName']),
                              params={'format': 'json'})

    def upload_fp(self, relation_key, schema, fp):
        """Upload the data in the supplied fp to the specified user and
        relation.

        Args:
            relation_key: A dictionary containing the destination relation key.
            schema: A dictionary containing the schema,
            fp: A file pointer containing the data to be uploaded.
        """

        # Clone the relation key and schema to ensure they don't contain
        # extraneous fields.
        relation_key = {'userName': relation_key['userName'],
                        'programName': relation_key['programName'],
                        'relationName': relation_key['relationName']}
        schema = {'columnTypes': schema['columnTypes'],
                  'columnNames': schema['columnNames']}

        data = base64.b64encode(fp.read())

        body = json.dumps({
            'relationKey': relation_key,
            'schema': schema,
            'source': {
                'dataType': 'Bytes',
                'bytes': data
            }})

        return self._make_request(POST, '/dataset', body)

    def submit_query(self, query):
        """Submit the query to Myria, and return the status including the URL
        to be polled.

        Args:
            query: a Myria physical plan as a Python object.
        """

        body = json.dumps(query)
        return self._wrap_post('/query', data=body)

    def execute_query(self, query):
        """Submit the query to Myria, and poll its status until it finishes.

        Args:
            query: a Myria physical plan as a Python object.
        """

        body = json.dumps(query)
        return self._finish_async_request(POST, '/query', body)

    def validate_query(self, query):
        """Submit the query to Myria for validation only.

        Args:
            query: a Myria physical plan as a Python object.
        """

        body = json.dumps(query)
        return self._make_request(POST, '/query/validate', body)

    def get_query_status(self, query_id):
        """Get the status of a submitted query.

        Args:
            query_id: the id of a submitted query
        """

        resource_path = '/query/query-%d' % int(query_id)
        return self._make_request(GET, resource_path)

    def get_fragment_ids(self, query_id, worker_id):
        """Get the number of fragments in a query plan.

        Args:
            query_id: the id of a submitted query
            worker_id: the id of a worker
        """
        status = self.get_query_status(query_id)
        if 'fragments' in status['physicalPlan']:
            fids = []
            for fragment in status['physicalPlan']['fragments']:
                if int(worker_id) in map(int, fragment['workers']):
                    fids.append(fragment['fragmentIndex'])
            return fids
        else:
            return []

    def get_sent_logs(self, query_id, fragment_id=None):
        """Get the logs for where data was sent.

        Args:
            query_id: the id of a submitted query
            fragment_id: the id of a fragment
        """
        resource_path = '/logs/sent?queryId=%d' % int(query_id)
        if fragment_id is not None:
            resource_path += '&fragmentId=%d' % int(fragment_id)
        response = self._make_request(GET, resource_path, accept=CSV)
        return csv.reader(response)

    def get_profiling_log(self, query_id, fragment_id=None):
        """Get the logs for operators.

        Args:
            query_id: the id of a submitted query
            fragment_id: the id of a fragment
        """
        resource_path = '/logs/profiling?queryId=%d' % int(query_id)
        if fragment_id is not None:
            resource_path += '&fragmentId=%d' % int(fragment_id)
        response = self._make_request(GET, resource_path, accept=CSV)
        return csv.reader(response)

    def get_profiling_log_roots(self, query_id, fragment_id):
        """Get the logs for root operators.

        Args:
            query_id: the id of a submitted query
            fragment_id: the id of a fragment
        """
        resource_path = '/logs/profilingroots?queryId=%d&fragmentId=%d' % (int(
            query_id), int(fragment_id))
        response = self._make_request(GET, resource_path, accept=CSV)
        return csv.reader(response)

    def queries(self, limit=None, max_=None):
        """Get count and information about all submitted queries.

        Args:
            limit: the maximum number of query status results to return.
            max_: the maximum query ID to return.
        """

        resource_path = '/query'
        args = {}
        if limit is not None:
            args['limit'] = limit
        if max_ is not None:
            args['max'] = max_
        r = (self._make_request(GET, resource_path,
             params=args, get_request=True))
        count = r.headers.get('x-count')
        assert count is not None, "Missing header: x-count"
        return int(count), r.json()
