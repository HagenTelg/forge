#!/bin/bash

STATION="$1"
if [ -z "${STATION}" ]; then
  echo "Station required as an argument" 1>&2
  exit 1
fi

if [ -z "${FORGE_ARCHIVE}" ]; then
  echo "No Forge archive environment variable set" 1>&2
  exit 2
fi
if [ -z "${CPD3ARCHIVE}" ]; then
  echo "No CPD3 archive environment variable set" 1>&2
  exit 3
fi
if ! which cpd3_forge_interface >/dev/null 2>&1; then
  echo "CPD3 Forge interface not in PATH" 1>&2
  exit 3
fi

unset CPD3_FORGE_LOOPBACK
export PYTHONDONTWRITEBYTECODE=1

echo "Starting legacy conversion for ${STATION}"

set -e
( cd raw; ./${STATION}.py )
( cd edits; ./${STATION}.py )
( cd eventlog; ./${STATION}.py )
( cd passed; ./${STATION}.py )

echo "Legacy conversion for ${STATION} complete"