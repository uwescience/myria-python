from setuptools import setup, find_packages

setup(
    name='myria-python',
    version='1.2.0',
    author='Daniel Halperin',
    author_email='dhalperi@cs.washington.edu',
    packages=find_packages(),
    scripts=[],
    url='https://github.com/uwescience/myria',
    description='Python interface for Myria.',
    long_description=open('README.md').read(),
    # see https://stackoverflow.com/questions/18578439
    install_requires=["pip >= 1.5.6", "pyOpenSSL >= 0.14", "ndg-httpsclient",
                      "pyasn1", "requests", "requests_toolbelt",
                      "messytables", "unicodecsv"],
    entry_points={
        'console_scripts': [
            'myria_upload = myria.cmd.upload_file:main'
        ],
    },
)
