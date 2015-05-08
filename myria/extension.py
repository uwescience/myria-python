""" IPython extensions for Myria queries """

import re

try:
    from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import Int, Unicode
    from IPython.display import HTML
    from IPython.core.magic_arguments import \
        argument, magic_arguments, parse_argstring

    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False

from myria import MyriaConnection, MyriaQuery, MyriaRelation


BIND_PATTERN = r'@(?P<identifier>[a-z_]\w*)'

if IPYTHON_AVAILABLE:
    @magics_class
    class MyriaExtension(Magics, Configurable):
        """ IPython extension for executing Myria queries """

        ambient_connection = None

        language = Unicode('MyriaL', config=True,
                           help='Language for Myria queries')
        timeout = Int(60, config=True, help='Query timeout (in seconds)')
        rest_url = Unicode('https://rest.myria.cs.washington.edu:1776',
                           config=True, help='Myria REST API endpoint URL')
        execution_url = Unicode('https://demo.myria.cs.washington.edu',
                                config=True, help='Myria web API endpoint URL')

        def __init__(self, shell):
            Configurable.__init__(self, config=shell.config)
            Magics.__init__(self, shell=shell)

            MyriaRelation.DefaultConnection = MyriaConnection(
                rest_url=self.rest_url,
                execution_url=self.execution_url,
                timeout=self.timeout)

            self.shell.configurables.append(self)

        @line_magic('connect')
        @magic_arguments()
        @argument('rest_url', type=str,
                  help='A Myria REST URL for query processing')
        @argument('execution_url', nargs='?', type=str, default=None,
                  help='A Myria-Web URL for program execution')
        @argument('-l', '--language', default='MyriaL', type=str,
                  help='Default query language (e.g., MyriaL, Datalog, SQL).')
        @argument('-t', '--timeout', default=60, type=int,
                  help='Default query timeout')
        def connect(self, line):
            """ Connect to a Myria REST (and optionally web) server """
            arguments = parse_argstring(self.connect, line)
            self.timeout = arguments.timeout
            self.language = arguments.language
            MyriaRelation.DefaultConnection = MyriaConnection(
                rest_url=arguments.rest_url,
                execution_url=arguments.execution_url,
                timeout=arguments.timeout)

            return MyriaRelation.DefaultConnection

        @line_magic('query')
        @cell_magic('query')
        def query(self, line, cell='', environment=None, language=None):
            """ Execute a Myria query using the current language.

                Relies on MyriaRelation.DefaultConnection, which may be
                set explicitly or via %connect.

                Examples:

                %language MyriaL
                %%query
                T1 = scan(TwitterK);
                T2 = [from T1 emit $0 as x];
                store(T2, JustX);

                %language Datalog
                %query JustX(column0) :- TwitterK(column0,column1)

                q = %query JustX(column0) :- TwitterK(column0,column1)%
            """
            self.shell.user_ns.update(environment or {})

            return MyriaQuery.submit(
                _bind(line + '\n' + cell, self.shell.user_ns),
                connection=MyriaRelation.DefaultConnection,
                language=language or self.language,
                timeout=self.timeout)

        @line_magic('plan')
        @cell_magic('plan')
        def plan(self, line, cell='', environment=None, language=None):
            """ Get a physical Myria plan for a program using the current language.

                Relies on MyriaRelation.DefaultConnection, which may be
                set explicitly or via %connect.

                Examples:

                %language MyriaL
                %%plan
                T1 = scan(TwitterK);
                T2 = [from T1 emit $0 as x];
                store(T2, JustX);

                p = %plan JustX(column0) :- TwitterK(column0,column1)%
            """
            self.shell.user_ns.update(environment or {})

            return MyriaRelation.DefaultConnection.compile_program(
                _bind(line + '\n' + cell, self.shell.user_ns),
                language=language or self.language)

        @line_magic('myrial')
        @cell_magic('myrial')
        def query_myrial(self, line, cell='', environment=None):
            """ Execute a MyriaL query """
            return self.query(line, cell, environment, language='MyriaL')

        @line_magic('datalog')
        @cell_magic('datalog')
        def query_datalog(self, line, cell='', environment=None):
            """ Execute a Datalog query """
            return self.query(line, cell, environment, language='Datalog')

        @line_magic('sql')
        @cell_magic('sql')
        def query_sql(self, line, cell='', environment=None):
            """ Execute a SQL query """
            return self.query(line, cell, environment, language='SQL')

        @staticmethod
        @line_magic('profile')
        @cell_magic('profile')
        def profile(line, environment=None):
            """ Profile a Myria query by instance or query id """
            query = eval(line, environment)
            query_id = query.query_id if isinstance(query, MyriaQuery) \
                else int(query)
            return HTML('''<iframe style="width: 100%; height: 800px"
                            src="{}/profile?queryId={}"></iframe>'''.format(
                                MyriaRelation.DefaultConnection.execution_url,
                                query_id))


def load_ipython_extension(ipython):
    """ Register the Myria IPython extension """
    ipython.register_magics(MyriaExtension)


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
