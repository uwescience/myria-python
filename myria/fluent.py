# coding=utf-8

import hashlib

from raco import compile
from raco.algebra import Store, Select, Apply, Scan, CrossProduct, Sequence, \
    ProjectingJoin, UnionAll, Sink, GroupBy, \
    Limit, Intersection, Difference, Distinct, OrderBy, EmptyRelation
from raco.backends.logical import OptLogicalAlgebra
from raco.backends.myria import MyriaLeftDeepTreeAlgebra
from raco.backends.myria import compile_to_json
from raco.backends.myria.catalog import MyriaCatalog
from raco.expression import UnnamedAttributeRef, NamedAttributeRef, COUNT, \
    COUNTALL, SUM, AVG, STDEV, MAX, MIN, PythonUDF, StringLiteral
from raco.python import convert
from raco.python.exceptions import PythonConvertException
from raco.relation_key import RelationKey
from raco.scheme import Scheme
from raco.types import STRING_TYPE, BOOLEAN_TYPE

from myria.udf import MyriaPythonFunction, MyriaFunction


def _get_column_index(inputs, aliases, attribute):
    """
    Find the column index of for given attribute
    :param inputs: Input queries with schema used to match against
    :param aliases: An alias for each input for dotted references
    :param attribute:  The attribute to map to an index
    :return: An UnnamedAttributeRef mapped to the given attribute
    """
    # $0
    if isinstance(attribute, (int, long)):
        return UnnamedAttributeRef(attribute)
    # alias.attribute
    elif isinstance(attribute, basestring) and '.' in attribute:
        assert aliases
        alias, attribute = map(str.strip, attribute.split('.'))
        index = aliases.index(alias)
        # ProjectingJoin will not accept a NamedAttributeRef :(
        return UnnamedAttributeRef(
            sum(len(q.query.scheme()) for q in inputs[:index]) +
            NamedAttributeRef(attribute).get_position(
                inputs[index].query.scheme()))
    # attribute
    elif isinstance(attribute, basestring):
        # ProjectingJoin will not accept a NamedAttributeRef :(
        return UnnamedAttributeRef(
            NamedAttributeRef(attribute).get_position(
                sum((q.query.scheme() for q in inputs), Scheme())))
    # lambda t1, t2: t1.attribute
    elif callable(attribute):
        ref = convert(attribute, [q.query.scheme() for q in inputs])
        schema = sum((q.query.scheme() for q in inputs), Scheme())
        return UnnamedAttributeRef(ref.get_position(schema))


def _unique_name(query):
    """ Generate a unique relation name """
    return 'result_%s' % hashlib.md5(str(query)).hexdigest()


def _create_udf(source_or_ast_or_callable, schema, connection,
                name=None, out_type=None, multivalued=False):
    name = name or _unique_name(str(source_or_ast_or_callable))
    out_type = out_type or STRING_TYPE

    MyriaPythonFunction(source_or_ast_or_callable,
                        str(out_type),
                        name,
                        multivalued,
                        connection=connection).register()
    return PythonUDF(
        StringLiteral(name),
        out_type,
        *[StringLiteral(name) for scheme in schema
          for name in scheme.get_names()])


class MyriaFluentQuery(object):
    def __init__(self, parent, query, connection=None):
        """
        Create a new fluent query
        :param parent: The parent fluent instance used to create this one
        :param query: The query associated with this fluent instance
        :param connection: The connection associated with this fluent instance
        """
        self.parent = parent
        self.query = query
        self.connection = connection if connection else parent.connection
        self.catalog = MyriaCatalog(self.connection)
        self.result = None
        self.udfs = [f.to_dict()
                     for f in MyriaFunction.get_all(self.connection)]

    def _scan(self, components):
        """ Scan a relation with the given name components """
        return Scan(RelationKey(*components),
                    MyriaCatalog(self.connection).get_scheme(
                        RelationKey(*components)))

    def _store(self, relation):
        """ Store the result of a query """
        return MyriaFluentQuery(self, Store(
            RelationKey(relation if isinstance(relation, basestring)
                        else relation.name),
            self.query))

    def _empty(self, schema):
        return MyriaFluentQuery(self, EmptyRelation(Scheme(zip(schema.names,
                                                               schema.types))))

    def _sink(self):
        """ Abandon the results of a query """
        return MyriaFluentQuery(self, Sink(self.query))

    def select(self, *args, **kwargs):
        """ Perform a projection over the underlying query """
        types = kwargs.pop('types', {})
        multivalued = kwargs.pop('multivalued', {})
        positional_attributes = (
            [(arg, NamedAttributeRef(arg)) if isinstance(arg, basestring)
             else ('_' + str(index),
                   self._convert(arg,
                                 out_type=types.get(index),
                                 multivalued=multivalued.get(index)))
             for index, arg in enumerate(args)])
        named_attributes = (
            [(n, NamedAttributeRef(v)) if isinstance(v, basestring)
             else (n, self._convert(v,
                                    out_type=types.get(n),
                                    multivalued=multivalued.get(n)))
             for (n, v) in kwargs.items()])
        return MyriaFluentQuery(self,
                                Apply(positional_attributes + named_attributes,
                                      self.query))

    def where(self, predicate):
        """ Filter the query given a predicate """
        return MyriaFluentQuery(self, Select(
            self._convert(predicate, out_type=BOOLEAN_TYPE),
            self.query))

    def product(self, other):
        """ Generate the cross product of two queries """
        return MyriaFluentQuery(self, CrossProduct(left=self.query,
                                                   right=other.query))

    def join(self, other, predicate=None, aliases=None, projection=None):
        """
        Join two queries
        :param other: The query to join on
        :param predicate: A predicate used to select tuples in the result
        :param aliases: A set of input aliases for attribute selection
        :param projection: A set of columns to output from the join result
        """
        if not predicate:
            return self.product(other)

        attributes = [_get_column_index([self, other], aliases, attribute)
                      for attribute in projection or
                      xrange(len(self.query.scheme() + other.query.scheme()))]
        predicate = self._convert(predicate,
                                  [self.query.scheme(), other.query.scheme()],
                                  out_type=BOOLEAN_TYPE)
        return MyriaFluentQuery(
            self,
            ProjectingJoin(
                condition=predicate,
                output_columns=attributes,
                left=self.query,
                right=other.query))

    def count(self, attribute=None, groups=None):
        """ Count the tuples in the query """
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[COUNT(_get_column_index([self], [], attribute))
                            if attribute else COUNTALL()]))

    def sum(self, attribute, groups=None):
        """ Generate the sum of an attribute """
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[SUM(_get_column_index([self], [], attribute))]))

    def mean(self, attribute, groups=None):
        """ Generate the arithmetic mean of an attribute"""
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[AVG(_get_column_index([self], [], attribute))]))

    def average(self, attribute, groups=None):
        """ Generate the arithmetic mean of an attribute """
        return self.mean(attribute, groups)

    def stdev(self, attribute, groups=None):
        """ Generate the standard deviation of an attribute """
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[STDEV(_get_column_index([self], [], attribute))]))

    def max(self, attribute, groups=None):
        """ Generate the maximum value of an attribute """
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[MAX(_get_column_index([self], [], attribute))]))

    def min(self, attribute, groups=None):
        """ Generate the minimum value of an attribute """
        return MyriaFluentQuery(self, GroupBy(
            input=self.query,
            grouping_list=[_get_column_index([self], [], g)
                           for g in ((groups or [])
                                     if isinstance(groups or [], list)
                                     else [groups])],
            aggregate_list=[MIN(_get_column_index([self], [], attribute))]))

    def limit(self, n):
        """ Limit the query to n results """
        return MyriaFluentQuery(self, Limit(n, self.query))

    def intersect(self, other):
        """ Generate the intersection of two queries """
        return MyriaFluentQuery(self, Intersection(self.query, other))

    def distinct(self):
        """ Generate the distinct values in a query """
        return MyriaFluentQuery(self, Distinct(self.query))

    def order(self, attribute, *args, **kwargs):
        """
        Order a query by one or more attributes
        :param attribute An attribute on which to order
        :param args: Other attributes on which to order
        :param kwargs: ascending=[True|False]
        """
        attributes = [attribute] + list(args)
        return MyriaFluentQuery(self, OrderBy(
            self.query,
            sort_columns=[_get_column_index([self], [], g)
                          for g in ((attributes or [])
                                    if isinstance(attributes or [], list)
                                    else [attributes])],
            ascending=kwargs.get('ascending', True)))

    def __add__(self, other):
        """ Generate the union of tuples in a query """
        return MyriaFluentQuery(self, UnionAll([self.query, other.query]))

    def __sub__(self, other):
        """ Generate the difference of tuples in a query """
        return MyriaFluentQuery(self, Difference(self.query, other.query))

    def __str__(self):
        return str(self.query)

    def __repr__(self):
        return repr(self.query)

    def to_dict(self):
        return self.execute().to_dict()

    def to_dataframe(self, index=None):
        return self.execute().to_dataframe(index)

    def execute(self, relation=None):
        """
        Execute a query
        :param relation: The name of a relation in which the result is stored
        :return: A MyriaQuery instance that represents the executing query
        """
        from myria.query import MyriaQuery

        if not self.result:
            json = self._store(relation or _unique_name(self.query)).to_json()
            self.result = MyriaQuery.submit_plan(json, self.connection)
        return self.result

    def sink(self):
        """ Execute the query but ignore its results """
        from myria.query import MyriaQuery
        return MyriaQuery.submit_plan(self._sink().to_json(), self.connection)

    def to_json(self):
        """ Convert this query into an optimized JSON plan """
        # TODO deep copy, since optimize mutates
        sequence = Sequence([self.query])
        optimized = compile.optimize(sequence, OptLogicalAlgebra())
        myria = compile.optimize(optimized, MyriaLeftDeepTreeAlgebra())
        return compile_to_json(str(self.query), optimized, myria)

    def _convert(self, source_or_ast_or_callable,
                 scheme=None, out_type=None, multivalued=False):
        scheme = scheme or [self.query.scheme()]
        try:
            return convert(source_or_ast_or_callable, scheme, udfs=self.udfs)
        except PythonConvertException:
            udf = _create_udf(source_or_ast_or_callable, scheme,
                              connection=self.connection,
                              out_type=out_type,
                              multivalued=multivalued)
            self.udfs.append([udf.name, len(udf.arguments), udf.typ])
            return udf
