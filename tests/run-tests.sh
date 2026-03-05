#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Install test dependencies
pip install -q -r "requirements.txt"

# Run all tests with verbose output
python -m pytest -v -p no:openmetadata "$@"
