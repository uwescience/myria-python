""" Utilities for generating Myria plans """

from functools import partial

DEFAULT_SCAN_TYPE = 'FileScan'
DEFAULT_INSERT_TYPE = 'DbInsert'
DEFAULT_READER_TYPE = 'CSV'


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

        'reader': {'schema': schema.to_dict(),
                   'readerType': DEFAULT_READER_TYPE},
        'source': datasource
    }
    scan['reader'].update(scan_parameters or {})

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
