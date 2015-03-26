""" Utilities for generating Myria plans """

from functools import partial


DEFAULT_SCAN_TYPE = 'FileScan'
DEFAULT_INSERT_TYPE = 'DbInsert'


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
    return {"overrideWorkers": [worker_id],
            "operators": [
                dict({
                     "opId": __increment(taskid),
                     "opType": scan_type or DEFAULT_SCAN_TYPE,

                     "schema": schema.to_json(),
                     "source": datasource
                     }.items() + (scan_parameters or {}).items()),
                dict({
                     "opId": __increment(taskid),
                     "opType": insert_type or DEFAULT_INSERT_TYPE,

                     "argChild": taskid[0] - 2,
                     "argOverwriteTable": True,

                     "relationKey": relation
                     }.items() + (insert_parameters or {}).items())]}


def __increment(value):
    value[0] += 1
    return value[0] - 1
