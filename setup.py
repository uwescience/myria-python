from setuptools import setup, find_packages

setup(
    name='myria-python',
    version='1.1.2',
    author='Daniel Halperin',
    author_email='dhalperi@cs.washington.edu',
    packages=find_packages(),
    scripts=[],
    url='https://github.com/uwescience/myria',
    description='Python interface for Myria.',
    long_description=open('README.rst').read(),
    install_requires=["requests", "requests_toolbelt", "messytables", "unicodecsv"],
    entry_points={
        'console_scripts': [
            'myria_upload = myria.cmd.upload_file:main'
        ],
    },
)
