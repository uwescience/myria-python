import unittest
import myria.plans
from myria.schema import MyriaSchema

QUALIFIED_NAME = {'userName': 'public',
                  'programName': 'adhoc',
                  'relationName': 'relation'}
SCHEMA = MyriaSchema({'columnNames': ['column'],
                      'columnTypes': ['INT_TYPE']})
WORK = [(0, 'http://input-uri-0'), (1, 'http://input-uri-1')]


class TestPlans(unittest.TestCase):
    def test_parallel_plan(self):
        text = 'This is logical relational algebra'

        plan = myria.plans.get_parallel_import_plan(SCHEMA,
                                                    WORK,
                                                    QUALIFIED_NAME,
                                                    text=text)
        self.assertDictContainsSubset({'rawQuery': text,
                                       'logicalRa': text}, plan)
        self.assertEquals(len(plan['fragments']), len(WORK))

    def test_worker_assignment(self):
        plan = myria.plans.get_parallel_import_plan(SCHEMA,
                                                    WORK,
                                                    QUALIFIED_NAME)

        fragments = plan['fragments']
        workers = reduce(lambda a, f: a + f['overrideWorkers'], fragments, [])
        self.assertListEqual(workers, [worker for worker, _ in WORK])

    def test_scan(self):
        scan_type = 'UNITTEST-SCAN'
        scan_metadata = {'metadata': 'foo'}
        plan = myria.plans.get_parallel_import_plan(
            SCHEMA, WORK, QUALIFIED_NAME,
            scan_type=scan_type,
            scan_metadata=scan_metadata)

        for fragment in plan['fragments']:
            scan_operator = fragment['operators'][0]

            self.assertEquals(scan_operator['opType'], scan_type)
            self.assertEquals(scan_operator['metadata'], 'foo')

    def test_insert(self):
        insert_type = 'UNITTEST-INSERT'
        insert_metadata = {'metadata': 'bar'}
        plan = myria.plans.get_parallel_import_plan(
            SCHEMA, WORK, QUALIFIED_NAME,
            insert_type=insert_type,
            insert_metadata=insert_metadata)

        for fragment in plan['fragments']:
            insert_operator = fragment['operators'][-1]

            self.assertEquals(insert_operator['opType'], insert_type)
            self.assertEquals(insert_operator['metadata'], 'bar')
