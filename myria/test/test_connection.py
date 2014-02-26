import StringIO
import unittest
from myria import MyriaConnection


class TestDeployment(unittest.TestCase):
    def test_no_deployment(self):
        assert MyriaConnection._parse_deployment(None) is None

    def test_deployment_file(self):
        deploy_file = StringIO.StringIO()
        deploy_file.write("""# Deployment configuration
[deployment]
path = /tmp/myria
name = twoNodeLocalParallel
dbms = sqlite
# Uncomment to set the maximum heap size of the Java processes
#max_heap_size=-Xmx2g
rest_port = 8753

# Compute nodes configuration
[master]
0 = localhost:8001

[workers]
1 = localhost:9001
2 = localhost:9002
""")
        deploy_file.seek(0)

        hostname, port = MyriaConnection._parse_deployment(deploy_file)
        self.assertEqual(hostname, 'localhost')
        self.assertEqual(port, 8753)

        deploy_file.close()
