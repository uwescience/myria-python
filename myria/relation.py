""" Higher-level types for interacting with Myria relations """

from dateutil.parser import parse
from itertools import izip
from myria import MyriaConnection, MyriaError
from myria.schema import MyriaSchema


class MyriaRelation(object):
    """ Represents a relation in the Myria system """

    DefaultConnection = MyriaConnection(hostname='localhost', port=8753)

    def __init__(self, relation, connection=DefaultConnection, schema=None):
        """ Attach to an existing Myria relation, or create a new one

        relation: the name of the relation.  One of:
           * qualified components: {'userName': 'public',
                                    'programName': 'adhoc',
                                    'relationName': 'my_relation'}
           * qualified name:       'public:adhoc:my_relation'
           * unqualified name:     'my_relation' (assume public:adhoc)

        Keyword arguments:
        connection: attach to a specific Myria API endpoint
        schema: for a relation that does not yet exist, specify its schema
        """
        self.name = relation if isinstance(relation, basestring) \
            else relation.name
        self.components = self._get_name_components(self.name)
        self.connection = connection
        self.qualified_name = self._get_qualified_name(self.components)
        self._schema = schema
        self._metadata = None

        if self._schema is not None and self.is_persisted:
            raise ValueError('New relation specified (schema != None), '
                             ' but it already exists on the server.')

    def to_json(self):
        """ Download this relation as JSON """
        return self.connection.download_dataset(self.qualified_name) \
            if self.is_persisted else None

    @property
    def schema(self):
        """ The schema of the relation """
        if self._schema is None:
            self._schema = MyriaSchema(json=self.metadata['schema'])
        return self._schema

    @property
    def created_date(self):
        """ The creation date for this relation """
        return parse(self.metadata['created'])

    def __len__(self):
        """ The number of tuples in the relation """
        return int(self.metadata['numTuples'])

    @property
    def metadata(self):
        """ A JSON dictionary of relation metadata """
        if self._metadata is None:
            self._metadata = self.connection.dataset(self.qualified_name)
        return self._metadata

    @property
    def is_persisted(self):
        """ Does the relation exist in the Myria database? """
        try:
            return bool(self.metadata)
        except MyriaError:
            return False

    @staticmethod
    def _get_name(qualified_name):
        """ Stringify a list of name components into a valid Myria name """
        return ':'.join([qualified_name['userName'],
                         qualified_name['programName'],
                         qualified_name['relationName']])

    @staticmethod
    def _get_name_components(name):
        """ Unstrigify a Myria relation name into a list of components """
        components = name.split(':')
        default_components = ['public', 'adhoc'][:max(3 - len(components), 0)]
        return default_components + components[:3]

    @staticmethod
    def _get_qualified_name(name_or_components):
        """ Generate a Myria relation dictionary from a string or list """
        if isinstance(name_or_components, basestring):
            return MyriaRelation._get_qualified_name(
                MyriaRelation._get_name_components(name_or_components))
        else:
            return dict(izip(('userName', 'programName', 'relationName'),
                             name_or_components[:3]))
