from setuptools import setup, find_packages

setup(
    name='myria-python',
    #namespace_packages=['myria'],
    version='1.3.4',
    author='Brandon Haynes, Daniel Halperin',
    author_email='bhaynes@cs.washington.edu',
    packages=find_packages(),
    scripts=[],
    url='https://github.com/uwescience/myria',
    description='Python interface for Myria.',
    long_description=open('README.md').read(),
    # last known good version before buggy dependency refactoring
    setup_requires=["requests == 2.20.0"],
    # see https://stackoverflow.com/questions/18578439
    install_requires=["pip >= 1.5.6", "pyOpenSSL >= 0.14", "ndg-httpsclient",
                      "pyasn1", "requests == 2.20.0", "requests_toolbelt",
                      "messytables", "unicodecsv", "raco >= 1.3.2",
                      "python-dateutil"],
    entry_points={
        'console_scripts': [
            'myria_upload = myria.cmd.upload_file:main'
        ],
    },
)
