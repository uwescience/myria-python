""" Utilities for generating Myria plans """

from functools import partial


def get_parallel_import_plan(schema, work, relation, text='',
                             scan_metadata=None, insert_metadata=None,
                             scan_type='FileScan', insert_type='DbInsert'):
    """ Generate a valid JSON Myria plan for parallel import of data

    work: list of (worker-id, data-source) pairs; data-source should be a
          JSON data source encoding
    relation: dict containing a qualified Myria relation name

    Keyword arguments:
      text: description of the plan
      scan_metadata: dict of additional operator parameters for the scan
      insert_metadata: dict of additional operator parameters for the insertion
      scan_type: type of scan to perform
      insert_Type: type of insert to perform
    """
    return \
        {"fragments": map(partial(_get_parallel_import_fragment, [0],
                                  schema, relation,
                                  scan_type, insert_type,
                                  scan_metadata, insert_metadata), work),
         "logicalRa": text,
         "rawQuery": text}


def _get_parallel_import_fragment(taskid, schema, relation,
                                  scan_type, insert_type,
                                  scan_metadata, insert_metadata,
                                  assignment):
    """ Generate a single fragment of the parallel import plan """
    worker_id = assignment[0]
    datasource = assignment[1]
    return {"overrideWorkers": [worker_id],
            "operators": [
                dict({
                     "opId": __increment(taskid),
                     "opType": scan_type,

                     "schema": schema.to_json(),
                     "source": datasource
                     }.items() + (scan_metadata or {}).items()),
                dict({
                     "opId": __increment(taskid),
                     "opType": insert_type,

                     "argChild": taskid[0] - 1,
                     "argOverwriteTable": True,

                     "relationKey": relation
                     }.items() + (insert_metadata or {}).items())
              ]
            }


def __increment(value):
    value[0] += 1
    return value[0] - 1
