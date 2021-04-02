#!/bin/bash
set -eou pipefail
set -x

export PORT=2355
export TIMEOUT="100s"
DATA="${DATA:-tests/state/webster.txt}"

./pinch-server
URL="localhost:${PORT}"

FD=$(curl -s "${URL}/pinch?min-level=1&timeout=200" | jq '.handles | keys[0]' -r)
RFD=$(curl -s "${URL}/unpinch?timeout=200" | jq '.handles | keys[0]' -r)

# Round trip compress and decompress
BEFORE=$(wc -c ${DATA})
AFTER=$(curl -svT tests/state/webster.txt -v "${URL}/io/${FD}?c&rw" | curl -svT - "${URL}/io/${RFD}?c&rw" | wc -c)
echo $BEFORE
echo $AFTER

curl -s "$URL/status/$FD"
