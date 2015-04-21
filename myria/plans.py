""" Utilities for generating Myria plans """

from functools import partial
from itertools import chain
from collections import namedtuple
import json
import jmespath
from myria import MyriaRelation

DEFAULT_SCAN_TYPE = 'FileScan'
DEFAULT_INSERT_TYPE = 'DbInsert'

# These shouldn't be in caps
HTML_STYLESHEETS = ''.join(['<link href="{{url}}/{}" />'.format(path) for path in ['css/bootstrap.min.css', 'css/queryvis.css']])
HTML_SCRIPTS = ''.join(['[script src="{{url}}/{}">[/script>'.format(path) for path in ['js/jquery-2.1.1.min.js', 'js/d3.min.js', 'js/lodash.min.js', 'js/bootstrap.min.js', 'js/jquery.panzoom.min.js', 'js/jquery.timeago.js', 'js/d3.selection.popover.js', 'js/graph.js', 'js/queryvis.js', 'js/viz.js']])
HTML_TEMPLATE = """
    <iframe id="plan-frame" src="about:blank" style="width: 100%; min-height: 360px; border: none"></iframe>

    <script id="plan-content" type="text/template">
        {stylesheets}
        {scripts}
        <style type="text/css">
            svg {{{{ padding: 3em 0 0 3em; }}}}
            .query-graph .node .node-label rect, .map .bg-rect {{{{ fill: white; opacity: 0.8; }}}}
        </style>
        [script type="text/javascript">
            $(document).ready(function() {{{{
                var myriaConnection = "{{url}}";
                var graph = new Graph();
                graph.loadQueryPlan('', {{fragments}});
                graph.render(d3.select('body'));
            }}}});
        [/script>
    </script>
    <script type="text/javascript">
        var d = document.getElementById('plan-frame').contentWindow.document;
        d.open();
        d.write($('#plan-content').html().replace(/\[(\/?)script/g, '<$1script'));
        d.close();
    </script>
    """.format(scripts=HTML_SCRIPTS, stylesheets=HTML_STYLESHEETS)

def get_html(connection, fragments):
    fragments = list(fragments)
    for f in fragments:
        if 'fragmentIndex' not in f.data:
            f.data['fragmentIndex'] = ''
    return HTML_TEMPLATE.format(
        url=connection.execution_url,
        fragments=json.dumps([f.data for f in fragments]))


class MyriaPlan(object):
    expressions = {
        'type': jmespath.compile('plan.type || plan.body.type'),
        'subplans': jmespath.compile('plan.plans[] || plans[] || plan.body[] || body[]'),
        'fragments': jmespath.compile('plan.fragments[] || fragments[]'),
        'fragment-list': jmespath.compile('plan.fragments || fragments'),
        'operators': jmespath.compile('plan.fragments[].operators[]'),
        'shuffles': jmespath.compile('plan.fragments[].operators[?contains(opType, `Shuffle`)][contains(opType, `Producer`)]')
        }

    @staticmethod
    def compile(program, language='MyriaL', connection=None):
        return MyriaPlan((connection or MyriaRelation.DefaultConnection).compile_program(program, language))

    def __init__(self, data, parent=None, connection=None):
        self.connection = connection
        self.parent = parent
        self.data = data

    @property
    def type(self):
        return self.expressions['type'].search(self.data)

    @property
    def language(self):
        return self.data['language']

    @property
    def profiling_mode(self):
        return self.data['profilingMode']

    @property
    def text(self):
        return self.data.get('rawQuery', self.logicalRa)

    @property
    def logicalRa(self):
        return self.data.get('logicalRa', self.data)

    @property
    def root(self):
        return self.parent.root if self.parent else self

    @property
    def subplans(self):
        return (MyriaPlan(subplan, parent=self, connection=self.connection) for subplan in self.expressions['subplans'].search(self.data) or [])

    @property
    def fragments(self, descendants=True):
        return chain((MyriaFragment(self, fragment) for fragment in self.expressions['fragments'].search(self.data) or []),
                     (fragment for subplan in self.subplans for fragment in subplan.fragments) if descendants else [])

    @property
    def operators(self, descendants=True):
        return chain((operator for fragment in self.fragments for operator in fragment.operators),
                     (operator for subplan in self.subplans for operator in subplan.operators) if descendants else [])

    @property
    def shuffles(self):
        return (operator for fragment in self.fragments for operator in fragment.shuffles)

    def search(self, path):
        return jmespath.search(path, self.data)

    @property
    def _fragment_list(self):
        return self.expressions['fragment-list'].search(self.data)

    def __str__(self):
        text = self.text or self.data
        return "MyriaPlan(%s)" % (text[:80] + '\n...' if len(text) > 80 else text)

    def __repr__(self):
        return self.__str__()

    def _repr_html_(self):
        """ Generate a representation of this plan as HTML """
        return get_html(self.connection or MyriaRelation.DefaultConnection, self.fragments)


class MyriaFragment(object):
    expressions = {
        'operators': jmespath.compile('operators[]'),
        'operator-list': jmespath.compile('operators'),
        'shuffles': jmespath.compile('operators[?contains(opType, `ShuffleProducer`)]')
        }

    def __init__(self, plan, data):
        self.plan = plan
        self.data = data

    @property
    def workers(self):
        return self.data.get('overrideWorkers', None)

    @property
    def operators(self):
        return (MyriaOperator(self, operator) for operator in self.expressions['operators'].search(self.data))

    @property
    def shuffles(self):
        return (MyriaOperator(self, operator) for operator in self.expressions['shuffles'].search(self.data))

    def merge(self, other, force=False):
        if self.workers != other.workers and not force:
            raise Exception('change me')
        self._operator_list.extend(other._operator_list)
        other.plan._fragment_list.remove(other.data)
        other.plan, other.data = self.plan, self.data

    @property
    def _operator_list(self):
        return self.expressions['operator-list'].search(self.data)

    def _repr_html_(self):
        """ Generate a representation of this fragment as HTML """
        return get_html(self.plan.connection or MyriaRelation.DefaultConnection, [self])


class MyriaOperator(object):
    REMOVABLE_OPERATORS = ['ShuffleProducer', 'ShuffleConsumer']
    CHILD_ARGUMENT_PREFIXES = ['argChild', 'argOperatorId']

    def __init__(self, fragment, data):
        self.fragment = fragment
        self.data = data

    @property
    def id(self):
        return self.data['opId']

    @property
    def name(self):
        return self.data['opName']

    @property
    def type(self):
        return self.data['opType']

    @property
    def plan(self):
        return self.fragment.plan

    @property
    def parent(self):
        return next((op for op in self.plan.operators
                        for child in op.children
                        if self.id == child.id), None)

    @property
    def children(self):
        return (op for op in self.plan.operators
                   if op.id in self._child_ids.values())

    @property
    def _child_ids(self):
        # Is this better than explicitly enumerating child attribute names?
        return {key: value for key, value in self.items()
                    if any(token in key for token in self.CHILD_ARGUMENT_PREFIXES) and
                       isinstance(value, int)}

    def remove(self, force=False):
        if len(self._child_ids) > 1:
            raise Exception('change type')
        elif self.type not in self.REMOVABLE_OPERATORS and not force:
            raise Exception('change' + self.type)
        else:
            key = (key for key, value in self.parent._child_ids.items() if value == self.id).next()
            self.parent[key] = self.children.next().id
            self.fragment._operator_list.remove(self.data)
            self.fragment = None

    def add(self, key, value): self.data.add(key, value)
    def items(self): return self.data.items()
    def keys(self): return self.data.keys()
    def values(self): return self.data.values()
    def __len__(self): return len(self.data)
    def __iter__(self): return self.data.__iter__()
    def __getitem__(self, item): return self.data[item]
    def __setitem__(self, item, value): self.data[item] = value
    def __delitem__(self, key): del self.data[key]
    def __contains__(self, item): return self.data.contains(item)

    def __str__(self):
        return "<myria.plans.MyriaOperator(%s: %s)>" % (self.type, self.name)

    def __repr__(self):
        return self.__str__()

    #TODO allow display of nth ancestors/descendants, rather than just n=1
    def neighborhood(self):
        fragment_data = {"operators": [self.data] + ([self.parent.data] if self.parent else []) + [c.data for c in self.children]}
        # need a _create_plan function that accepts fragments for this and repr_html
        plan_data = {'plan': {'type': self.plan.type,
                              'fragments': [fragment_data],
                              'language': self.plan.language}}
        return MyriaPlan(plan_data, connection=self.plan.connection).fragments.next()

    def _repr_html_(self):
        """ Generate a representation of this plan as HTML """
        pseudo_fragment = namedtuple('MyriaPseudoFragment', ['data'])
        return get_html(self.plan.connection or MyriaRelation.DefaultConnection, [pseudo_fragment({"operators": [self.data]})])


def remove_shuffle(producer):
    consumer = producer.parent

    if 'ShuffleProducer' not in producer.type:
        raise Exception('asdf')
    elif consumer is None or 'ShuffleConsumer' not in consumer.type:
        raise Exception('asdf')

    _remove_merge(producer)


def _remove_merge(child, parent=None):
    parent = parent or child.parent

    if parent is None:
        raise Exception('asdf')
    elif parent.fragment.workers != child.fragment.workers:
        raise Exception('asdf')

    parent.fragment.merge(child.fragment)
    child.remove()
    parent.remove()


def get_parallel_import_plan(schema, work, relation, text='',
                             scan_parameters=None, insert_parameters=None,
                             scan_type=None, insert_type=None):
    """ Generate a valid JSON Myria plan for parallel import of data

    work: list of (worker-id, data-source) pairs; data-source should be a
          JSON data source encoding
    relation: dict containing a qualified Myria relation name

    Keyword arguments:
      text: description of the plan
      scan_parameters: dict of additional operator parameters for the scan
      insert_parameters: dict of additional operator parameters for insertion
      scan_type: type of scan to perform
      insert_Type: type of insert to perform
    """
    return \
        {"fragments": map(partial(_get_parallel_import_fragment, [0],
                                  schema, relation,
                                  scan_type, insert_type,
                                  scan_parameters, insert_parameters), work),
         "logicalRa": text,
         "rawQuery": text}


def _get_parallel_import_fragment(taskid, schema, relation,
                                  scan_type, insert_type,
                                  scan_parameters, insert_parameters,
                                  assignment):
    """ Generate a single fragment of the parallel import plan """
    worker_id = assignment[0]
    datasource = assignment[1]

    scan = {
        'opId': __increment(taskid),
        'opType': scan_type or DEFAULT_SCAN_TYPE,

        'schema': schema.to_dict(),
        'source': datasource
    }
    scan.update(scan_parameters or {})

    insert = {
        'opId': __increment(taskid),
        'opType': insert_type or DEFAULT_INSERT_TYPE,

        'argChild': taskid[0] - 2,
        'argOverwriteTable': True,

        'relationKey': relation
    }
    insert.update(insert_parameters or {})

    return {'overrideWorkers': [worker_id],
            'operators': [scan, insert]}


def __increment(value):
    value[0] += 1
    return value[0] - 1
