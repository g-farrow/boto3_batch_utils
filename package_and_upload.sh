#!/usr/bin/env bash

read -n 1 -s -r -p "Have you increased the version number? "

read -n 1 -s -r -p "Old distributions should be removed from 'dist/'. Press a key to continue... "

echo -e "\nPackaging up the library using 'python setup.py sdist'"
python setup.py sdist

echo -e "\nUploading the package to PIP using 'twine upload dist/*'"
twine upload dist/*
