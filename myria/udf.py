"""Creating User Defined functions"""
import re
import base64
from itertools import imap
from raco.backends.myria.connection import FunctionTypes
from raco.python.exceptions import PythonConvertException
from raco.python.util.decompile import get_source
from raco.types import STRING_TYPE

from myria.utility import cloudpickle


def myria_function(name=None, output_type=STRING_TYPE, multivalued=False,
                   connection=None):
    def decorator(f):
        from myria import MyriaFluentQuery
        udf_name = name or f.__name__

        if connection:
            MyriaPythonFunction(f, output_type, name,
                                multivalued, connection).register()

        setattr(
            MyriaFluentQuery,
            udf_name,
            lambda self: self.select(
                **{udf_name: f,
                   'types': {udf_name: output_type},
                   'multivalued': {udf_name: multivalued}}))

    return decorator


class MyriaFunction(object):
    _cache = {}

    @classmethod
    def get_all(cls, connection):
        if connection.execution_url not in cls._cache:
            cls._cache[connection.execution_url] = [
                MyriaPythonFunction.from_dict(udf, connection)
                if udf['lang'] == FunctionTypes.PYTHON else
                MyriaPostgresFunction.from_dict(udf, connection)
                for udf in imap(connection.get_function,
                                connection.get_functions())]
        return cls._cache[connection.execution_url]

    @classmethod
    def get(cls, name, connection):
        return next((f for f in cls.get_all(connection) if f.name == name),
                    None)

    def __init__(self, name, source, output_type, language, multivalued,
                 connection=None):
        self.connection = connection
        self.name = name
        self.source = source
        self.output_type = output_type
        self.multivalued = multivalued
        self.language = language

    def register(self, overwrite_if_exists=True):
        from myria import MyriaRelation
        connection = self.connection or MyriaRelation.DefaultConnection
        self.get_all(connection).append(self)
        connection.create_function(self.to_dict(),
                                   overwrite_if_exists=overwrite_if_exists)

    def to_dict(self):
        return {'name': self.name,
                'description': self.source,
                'outputType': self.output_type,
                'isMultiValued': self.multivalued,
                'lang': self.language}


class MyriaPostgresFunction(MyriaFunction):
    def __init__(self, name, source, output_type,
                 multivalued=False, connection=None):
        super(MyriaPostgresFunction, self).__init__(
            name, source, output_type, FunctionTypes.POSTGRES,
            multivalued, connection)

    @staticmethod
    def from_dict(d, connection=None):
        from myria import MyriaRelation
        return MyriaPostgresFunction(
            d['name'],
            d.get('description', None),
            d['outputType'],
            bool(d.get('isMultiValued', False)),
            connection=connection or MyriaRelation.DefaultConnection)


class MyriaPythonFunction(MyriaFunction):
    def __init__(self, body, output_type=STRING_TYPE, name=None,
                 multivalued=False, connection=None):
        self.body = body
        self.binary = base64.urlsafe_b64encode(cloudpickle.dumps(body, 2))
        super(MyriaPythonFunction, self).__init__(
            self._get_name(name, body), self._get_source(body), output_type,
            FunctionTypes.PYTHON, multivalued, connection)

    def to_dict(self):
        d = super(MyriaPythonFunction, self).to_dict()
        d['binary'] = self.binary
        return d

    @staticmethod
    def _get_source(body):
        try:
            return get_source(body)
        except PythonConvertException:
            return None

    @staticmethod
    def _get_name(name, body):
        if name:
            return name
        elif re.match(r'^\w+$', body.__name__):
            return body.__name__
        else:
            raise PythonConvertException(
                'Could not automatically generate name for function ' +
                body.__name__)

    @staticmethod
    def from_dict(d, connection=None):
        from myria import MyriaRelation
        return MyriaPythonFunction(
            eval(d.get('source', "0")),
            d['outputType'],
            d['name'],
            bool(d.get('isMultiValued', False)),
            connection=connection or MyriaRelation.DefaultConnection)
