""" IPython extensions for Myria queries """

import re
from urlparse import urlparse
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.config.configurable import Configurable
from IPython.utils.traitlets import Int, Unicode
from IPython.display import HTML
from myria import *


BIND_PATTERN = r'[^\w]:(?P<identifier>[a-z_]\w*)'


@magics_class
class MyriaExtension(Magics, Configurable):
    """ IPython extension for executing Myria queries """

    ambient_connection = None

    language = Unicode('MyriaL', config=True,
                       help='Language for Myria queries')
    timeout = Int(60, config=True, help='Query timeout (in seconds)')
    url = Unicode('https://rest.myria.cs.washington.edu:1776', config=True,
                  help='Myria API endpoing URL')

    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        self.shell.configurables.append(self)

    @line_magic('myria')
    @cell_magic('myria')
    def execute(self, line, cell='', environment=None, profile=False):
        """ Execute a Myria query

        If no ambient connection exists, the first line should contain a
        port-qualified URI that points to the Myria server.  It may also
        (optionally) contain the query language.  For example:

            %myria MyriaL, https://rest.myria.cs.washington.edu:1776

        This connection is remembered across future queries, and only need
        be specified the first time (or when connection metadata changes).

        You may also embed both a language and query in the same block:

            %%myria MyriaL
            T1 = scan(TwitterK);
            T2 = [from T1 emit $0 as x];
            store(T2, JustX);

            %%myria Datalog
            JustX(column0) :- TwitterK(column0,column1)

            %%myria Datalog, https://rest.myria.cs.washington.edu:1776
            JustX(column0) :- TwitterK(column0,column1)
        """
        # Connection specified inline is considered first
        if line:
            self._parse_metadata(line)
        # Next, consider a default connection via config
        if not self.ambient_connection and self.url:
            self.ambient_connection = _create_connection(self.url)
        # Neither instantiated a connection, so abort.
        if not self.ambient_connection:
            raise ValueError('No connection metadata specified, and no ambient'
                             ' connection exists (try '
                             '"%%myria http://server:port")')

        if cell:
            self.shell.user_ns.update(environment or {})

            return MyriaQuery.submit(_bind(cell, self.shell.user_ns),
                                     connection=self.ambient_connection,
                                     language=self.language,
                                     profile=profile,
                                     timeout=self.timeout)

    @line_magic('profile')
    @cell_magic('profile')
    def profile(self, line, cell='', environment=None):
        query = eval(line, environment)
        return HTML('<iframe style="width: 100%; height: 600px" src="https://demo.myria.cs.washington.edu/profile?queryId={}"></iframe>'.format(query.query_id))


    def _parse_metadata(self, metadata):
        """ Parse a metadata line and set ambient connection and values
            Format should be one of:

            "[language]"
            "[language], [url]"
        """
        tokens = map(unicode.strip, metadata.split(','))
        if tokens:
            self.language = tokens[0]
        if len(tokens) > 1:
            self.ambient_connection = _create_connection(tokens[1])


def load_ipython_extension(ipython):
    """ Register the Myria IPython extension """
    ipython.register_magics(MyriaExtension)


def _create_connection(url):
    """ Create a connection given a url string or ParseResult """
    url = urlparse(url) if isinstance(url, basestring) else url
    return MyriaConnection(hostname=url.hostname,
                           port=url.port or (80 if url.scheme == 'http' \
                               else 443),
                           ssl=url.scheme == 'https')


def _bind(query, environment):
    """ Perform variable binding on the given query.
        All identifiers of the form :identifier are matched and
        replaced with the corresponding value from the environment.
        Note that indexing (:identifier[0]) and member references
        (:identifier.foo.bar) are not yet supported.  Boo!
    """
    # Total laziness: reverse list to avoid index changes during mutation
    for match in reversed(list(re.finditer(BIND_PATTERN, query, re.I))):
        query = query[:match.start()] + \
                unicode(eval(match.group('identifier'), environment)) + \
                query[match.end():]
    return query
