#! /usr/bin/env bash

# call sphinx with paranoid options

sphinx-build -M html $1 $2 --fresh-env -W --nitpicky
