import subprocess
import sys
import unittest


def check_output_and_print_stderr(args):
    """Run the specified command. If it does not exit cleanly, print the stderr
    of the command to stderr"""
    try:
        subprocess.check_output(args, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print >> sys.stderr, e.output
        raise


class StyleTest(unittest.TestCase):
    def test_flake8(self):
        "run flake8 with the right arguments and ensure all files pass"
        check_output_and_print_stderr(['flake8', '--ignore=F', 'myria'])

    def test_pylint(self):
        "run pylint -E to catch obvious errors"
        check_output_and_print_stderr([
            'pylint',
            '-E',
            '--ignore=cloudpickle.py,test_fluent.py',
            'myria'])


    def test_errcatch(self):
        "make sure that we get an exception if the command does not exit 0"
        with self.assertRaises(subprocess.CalledProcessError):
            check_output_and_print_stderr(['false'])
