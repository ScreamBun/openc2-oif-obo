#!/usr/bin/env bash

echo "Running pylint on obo"
pylint --rcfile=.pylintrc --output-format=json obo | pylint-json2html -o "lint_obo.html"
