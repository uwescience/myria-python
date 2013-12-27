import base64
import ConfigParser
from collections import OrderedDict
import httplib
import json
from time import sleep
import urllib2

from .errors import MyriaError

__all__ = ['MyriaConnection']

# String constants used in forming requests
JSON = 'application/JSON'
GET = 'GET'
PUT = 'PUT'
POST = 'POST'

# httplib debug level
HTTPLIB_DEBUG_LEVEL = 0

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
                 port=None):
        """Initializes a connection to the Myria REST server.

        Args:
            deployment: An open file (or other buffer) containing a
                deployment.cfg file in Myria's format. This file will be parsed
                to determine the REST server hostname and port.
            hostname: The hostname of the REST server. May be overwritten if
                deployment is provided.
            port: The port of the REST server. May be overwritten if deployment
                is provided.
        """
        # Parse the deployment file and, if present, override the hostname and
        # port with any provided values from deployment.
        rest_config = self._parse_deployment(deployment)
        if rest_config is not None:
            hostname = hostname or rest_config[0]
            port = port or rest_config[1]

        self._connection = httplib.HTTPConnection(hostname, port)
        self._connection.set_debuglevel(HTTPLIB_DEBUG_LEVEL)
        self._connection.connect()
        # get workers just to make sure the connection is alive
        self.workers()

    def _finish_async_request(self, method, url, body=None, headers=None):
        try:
            if headers is None:
                headers = self._DEFAULT_HEADERS

            while True:
                self._connection.request(method, url, headers=headers, body=body)
                response = self._connection.getresponse()
                if response.status in [httplib.OK, httplib.CREATED]:
                    return json.load(response)
                elif response.status in [httplib.ACCEPTED]:
                    # Get the new URL to poll, etc.
                    url = response.getheader('Location')
                    method = GET
                    body = None
                    # Read and ignore the body
                    response.read()
                    # Sleep 100 ms before re-issuing the request
                    sleep(0.1)
                else:
                    raise MyriaError('Error %d (%s): %s'
                            % (response.status, response.reason, response.read()))
        except Exception as e:
            raise MyriaError(e)

    def _make_request(self, method, url, body=None, headers=None):
        try:
            if headers is None:
                headers = self._DEFAULT_HEADERS

            self._connection.request(method, url, headers=headers, body=body)
            response = self._connection.getresponse()
            if response.status in [httplib.OK, httplib.CREATED, httplib.ACCEPTED]:
                return json.load(response)
            else:
                raise MyriaError('Error %d (%s): %s'
                        % (response.status, response.reason, response.read()))
        except Exception as e:
            raise MyriaError(e)

    def workers(self):
        """Return a dictionary of the workers"""
        return self._make_request(GET, "/workers")

    def workers_alive(self):
        """Return a list of the workers that are alive"""
        return self._make_request(GET, "/workers/alive")

    def worker(self, worker_id):
        """Return information about the specified worker"""
        return self._make_request(GET, "/worker/worker-%d" % worker_id)

    def datasets(self):
        """Return a list of the datasets that exist"""
        return self._make_request(GET, "/dataset")

    def dataset(self, relation_key):
        """Return information about the specified relation"""
        url = "/dataset/user-%s/program-%s/relation-%s" % \
                (urllib2.quote(relation_key['user_name']),
                 urllib2.quote(relation_key['program_name']),
                 urllib2.quote(relation_key['relation_name']))
        return self._make_request(GET, url)

    def download_dataset(self, relation_key):
        """Download the data in the dataset as json"""
        url = "/dataset/user-%s/program-%s/relation-%s/data?format=json" % \
                (urllib2.quote(relation_key['user_name']),
                 urllib2.quote(relation_key['program_name']),
                 urllib2.quote(relation_key['relation_name']))
        return self._make_request(GET, url)

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
        relation_key = {'user_name': relation_key['user_name'],
                        'program_name': relation_key['program_name'],
                        'relation_name': relation_key['relation_name']}
        schema = {'column_types': schema['column_types'],
                  'column_names': schema['column_names']}

        data = base64.b64encode(fp.read())

        body = json.dumps({
            'relation_key': relation_key,
            'schema': schema,
            'source': {
                'data_type': 'Bytes',
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
        return self._make_request(POST, '/query', body)

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

    def queries(self):
        """Get information about all submitted queries.
        """

        resource_path = '/query'
        return self._make_request(GET, resource_path)
