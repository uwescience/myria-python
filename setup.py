try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='myria-python',
    version='1.0',
    author='Daniel Halperin',
    author_email='dhalperi@cs.washington.edu',
    packages=['myria'],
    scripts=[],
    url='https://github.com/uwescience/myria',
    description='Python interface for Myria.',
    long_description=open('README.txt').read(),
    install_requires=["requests", "requests_toolbelt", "messytables"],
)
