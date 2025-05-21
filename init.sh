#!/bin/bash

# Create virtual environment if it doesn't exist
if [ ! -d .venv_test ]; then
    python -m venv .venv_test
    source .venv_test/bin/activate
    pip install .
fi