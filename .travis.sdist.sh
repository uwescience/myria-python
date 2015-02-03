#!/bin/bash

python setup.py sdist && cd dist
if [[ $? -ne 0 ]]; then
	exit $?
fi

base=`basename *gz .tar.gz`
tar xzf ${base}.tar.gz && cd ${base}
if [[ $? -ne 0 ]]; then
	exit $?
fi

python setup.py install
