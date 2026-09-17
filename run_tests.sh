#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SCALELIB=${CYCLECLOUD_SCALELIB:-"$PROJECT_ROOT/../cyclecloud-scalelib"}
API_WHEELS=("$PROJECT_ROOT"/libs/cyclecloud_api-*.whl)
API_WHEEL=${CYCLECLOUD_API:-"${API_WHEELS[0]}"}

if [[ ! -e "$SCALELIB" ]]; then
    echo "Set CYCLECLOUD_SCALELIB to a local scalelib checkout or archive." >&2
    exit 1
fi
if [[ ! -f "$API_WHEEL" ]]; then
    echo "Set CYCLECLOUD_API to a local CycleCloud API wheel, or place one in libs/." >&2
    exit 1
fi

"${PYTHON:-python3}" -m venv "$PROJECT_ROOT/.testvenv"
TEST_PYTHON="$PROJECT_ROOT/.testvenv/bin/python"
"$TEST_PYTHON" -m pip install --upgrade pip 'setuptools<72' wheel
"$TEST_PYTHON" -m pip install --no-build-isolation \
    "$API_WHEEL" "$SCALELIB" -e "$PROJECT_ROOT/gridengine" pytest PyYAML

cd "$PROJECT_ROOT"
TEST_PATHS=(gridengine/test)
if [[ -d test ]]; then
    TEST_PATHS+=(test)
fi
exec "$TEST_PYTHON" -m pytest "${TEST_PATHS[@]}" "$@"