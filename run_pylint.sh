#!/usr/bin/env bash

DIRS=(
  "actuators"
  "transports"
)

for DIR in "${DIRS[@]}"; do
  echo "Running pylint on ${DIR}"
  pylint --rcfile=.pylintrc --output-format=json "$DIR" | pylint-json2html -o "lint_${DIR}.html"
done
