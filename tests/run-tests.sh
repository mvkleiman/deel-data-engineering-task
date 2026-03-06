#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Install test dependencies
pip install -q -r "requirements.txt"

# Run all tests with verbose output (I have a broken installation of openmetadata so I'm disabling it for the pytest command)
python -m pytest -v -p no:openmetadata "$@"
