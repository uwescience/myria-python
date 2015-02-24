from functools import partial

def get_parallel_import_plan(schema, work, relation, text='', 
                             scan_metadata={}, insert_metadata={}, 
                             scan_type='FileScan', insert_type='DbInsert'):
   return \
      { "fragments": map(partial(_get_parallel_import_fragment, [0], schema, relation, 
                                 scan_type, insert_type,
                                 scan_metadata, insert_metadata), work),
        "logicalRa": text, 
        "rawQuery":  text }

def _get_parallel_import_fragment(taskid, schema, relation, scan_type, insert_type,
                                  scan_metadata, insert_metadata, (worker, datasource)):
  return { "overrideWorkers": [worker],
           "operators":[
               dict({
                  "opId": __increment(taskid),
                  "opType": scan_type,

                  "schema": schema.toJson(),
                  "source": datasource
               }.items() + scan_metadata.items()),
               dict({
                  "opId": __increment(taskid),
                  "opType": insert_type,

                  "argChild": taskid[0]-1,
                  "argOverwriteTable": True,

                  "relationKey": relation
                }.items() + insert_metadata.items())
             ]
          }

def __increment(id):
  id[0] += 1
  return id[0] - 1