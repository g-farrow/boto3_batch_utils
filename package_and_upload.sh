#!/usr/bin/env bash

echo "Packaging up the library using 'python setup.py sdist'"
python setup.py sdist

echo "Uploading the package to PIP using 'twine upload dist/*'"
twine upload dist/*
