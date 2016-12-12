"""Creating User Defined functions"""
import base64

from raco.backends.myria.connection import FunctionTypes
from raco.python.util.decompile import get_source
from raco.types import STRING_TYPE

from myria.utility import cloudpickle


class TypeSignature(object):
    def __init__(self, _in, out):
        self.input_types = _in
        self.output_type = out


class MyriaFunction(object):
    _cache = {}

    @classmethod
    def get_all(cls, connection):
        if connection.execution_url not in cls._cache:
            cls._cache[connection.execution_url] = [
                MyriaPythonFunction.from_dict(udf, connection)
                if udf['lang'] == FunctionTypes.PYTHON else
                MyriaPostgresFunction.from_dict(udf, connection)
                for udf in connection.get_functions()]

        return cls._cache[connection.execution_url]

    @classmethod
    def get(cls, name, connection):
        return cls.get_all(connection).get(name, None)

    def __init__(self, name, source, signature, language, connection=None):
        self.connection = connection
        self.name = name
        self.source = source
        self.signature = signature
        self.language = language

    def register(self):
        from myria import MyriaRelation
        connection = self.connection or MyriaRelation.DefaultConnection
        self.get_all(connection).append(self)
        connection.create_function(self.to_dict())

    def to_dict(self):
        return {'name': self.name,
                'source': self.source,
                'outputType': self.signature.output_type,
                'inputSchema': self.signature.input_types,
                'lang': self.language}


class MyriaPostgresFunction(MyriaFunction):
    def __init__(self, name, source, signature, connection=None):
        super(MyriaPostgresFunction, self).__init__(
            name, source, signature, FunctionTypes.POSTGRES, connection)

    @staticmethod
    def from_dict(d, connection=None):
        from myria import MyriaRelation
        return MyriaPostgresFunction(
            d['name'],
            d.get('source', None),
            TypeSignature(d['inputSchema'], d['outputType']),
            connection=connection or MyriaRelation.DefaultConnection)


class MyriaPythonFunction(MyriaFunction):
    def __init__(self, name, out_type, body, arity, connection=None):
        self.body = body
        self.binary = base64.urlsafe_b64encode(cloudpickle.dumps(body, 2))
        scheme = [STRING_TYPE for _ in xrange(arity)]
        super(MyriaPythonFunction, self).__init__(
            name, get_source(body), TypeSignature(scheme, out_type),
            FunctionTypes.POSTGRES, connection)

    def to_dict(self):
        d = super(MyriaPythonFunction, self).to_dict()
        d['binary'] = self.binary
        return d

    @staticmethod
    def from_dict(d, connection=None):
        from myria import MyriaRelation
        return MyriaPythonFunction(
            d['name'], d['outputType'],
            eval(d.get('source', "0")),
            len(d['inputSchema']),
            connection=connection or MyriaRelation.DefaultConnection)
