#!/usr/bin/env bash

set -e

PROJECT_ROOT_DIR="${PWD}"
TEST_REQUIREMENT_FILE="${PROJECT_ROOT_DIR}/test_requirements.txt"

rm -rf test_env
python3 -m venv test_env
"${PROJECT_ROOT_DIR}/test_env/bin/python" -m pip install --upgrade pip
test_env/bin/pip install -r test_requirements.txt

cd "${PROJECT_ROOT_DIR}/tests" || exit 1

echo "Running Tests"
PYTHONPATH="${PROJECT_ROOT_DIR}/log_forwarder" "${PROJECT_ROOT_DIR}/test_env/bin/python" -m pytest -v -s
