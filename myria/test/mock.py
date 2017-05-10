import json
from datetime import datetime

from httmock import urlmatch
from myria.udf import MyriaPythonFunction
from raco.types import STRING_TYPE, LONG_TYPE

RELATION_NAME = 'relation'
FULL_NAME = 'public:adhoc:' + RELATION_NAME
QUALIFIED_NAME = {'userName': 'public',
                  'programName': 'adhoc',
                  'relationName': RELATION_NAME}
NAME_COMPONENTS = ['public', 'adhoc', RELATION_NAME]
SCHEMA = {'columnNames': ['column', 'column2'],
          'columnTypes': ['INT_TYPE', 'INT_TYPE']}
CREATED_DATE = datetime(1900, 1, 2, 3, 4)
TUPLES = [[1, 9], [2, 8], [3, 7], [4, 6], [5, 5]]
TOTAL_TUPLES = len(TUPLES)

RELATION_NAME2 = 'relation2'
FULL_NAME2 = 'public:adhoc:' + RELATION_NAME2
QUALIFIED_NAME2 = {'userName': 'public',
                   'programName': 'adhoc',
                   'relationName': RELATION_NAME2}
NAME_COMPONENTS2 = ['public', 'adhoc', RELATION_NAME2]
SCHEMA2 = {'columnNames': ['column3', 'column4'],
           'columnTypes': ['INT_TYPE', 'INT_TYPE']}
TUPLES2 = [[1, 9], [2, 8], [3, 7], [4, 6], [5, 5]]
TOTAL_TUPLES2 = len(TUPLES2)

UDF1_NAME, UDF2_NAME = 'udf1', 'udf2'
UDF1_TYPE, UDF2_TYPE = LONG_TYPE, STRING_TYPE
UDF1_ARITY, UDF2_ARITY = 1, 2


def get_uri(name):
    return '/dataset/user-{}/program-{}/relation-{}'.format(
        'public', 'adhoc', name)


def create_mock(state=None):
    state = state if state is not None else {}

    @urlmatch(netloc=r'localhost:12345')
    def local_mock(url, request):
        # Relation metadata
        if url.path == get_uri(RELATION_NAME):
            body = {'numTuples': TOTAL_TUPLES,
                    'schema': SCHEMA,
                    'created': str(CREATED_DATE)}
            return {'status_code': 200, 'content': body}
        elif url.path == get_uri(RELATION_NAME2):
            body = {'numTuples': TOTAL_TUPLES2,
                    'schema': SCHEMA2,
                    'created': str(CREATED_DATE)}
            return {'status_code': 200, 'content': body}

        # Relation download
        if url.path == get_uri(RELATION_NAME) + '/data':
            body = str(TUPLES)
            return {'status_code': 200, 'content': body}
        elif url.path == get_uri(RELATION_NAME2) + '/data':
            body = str(TUPLES2)
            return {'status_code': 200, 'content': body}

        # Relation not found in database
        elif get_uri('NOTFOUND') in url.path:
            return {'status_code': 404}

        elif url.path == '/function' and request.method == 'GET':
            body = [UDF1_NAME, UDF2_NAME]
            return {'status_code': 200, 'content': body}

        elif url.path == '/function/' + UDF1_NAME and request.method == 'GET':
            return {
                'status_code': 200,
                'content': MyriaPythonFunction(
                    lambda i: 0, UDF1_TYPE, UDF1_NAME, False).to_dict()}

        elif url.path == '/function/' + UDF2_NAME and request.method == 'GET':
            return {
                'status_code': 200,
                'content': MyriaPythonFunction(
                    lambda i: 0, UDF2_TYPE, UDF2_NAME, False).to_dict()}

        elif url.path == '/function' and request.method == 'POST':
            body = json.loads(request.body)
            state[body['name']] = body
            return {'status_code': 200, 'content': '{}'}

        elif url.path == '/query' and request.method == 'POST':
            state['query'] = json.loads(request.body)
            return {'status_code': 200,
                    'headers': {'Location': ""},
                    'content': {'queryId': 999}}


        return None
    return local_mock
