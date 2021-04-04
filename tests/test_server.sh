#!/bin/bash
set -eou pipefail
set -x

trap 'cleanup' EXIT

cleanup() {
    docker kill pinch-server || echo "server already dead"
}

cleanup

export PORT=2355
export DIEAFTER="100s"
DATA="${DATA:-tests/state/webster.txt}"

./pinch-server
URL="localhost:${PORT}"

FD=$(curl -s "${URL}/pinch?min-level=1&timeout=10s" | jq '.handles | keys[0]' -r)
RFD=$(curl -s "${URL}/unpinch?timeout=10s" | jq '.handles | keys[0]' -r)

# Round trip compress and decompress
BEFORE=$(cat ${DATA} | wc -c)
AFTER=$(curl -svT tests/state/webster.txt -v "${URL}/io/${FD}" | curl -svT - "${URL}/io/${RFD}" | wc -c)
echo $BEFORE
echo $AFTER

test "${BEFORE}" -eq "${AFTER}"

echo "Compress output"
curl -s "${URL}/status/${FD}" | jq ".stderr" -r
COMP_BLAKE3=$(curl -s "${URL}/status/${FD}" | jq ".checksums.blake3" -r)
COMP_XXH128=$(curl -s "${URL}/status/${FD}" | jq ".checksums.xxh128" -r)
echo "Decompress output"
curl -s "${URL}/status/${RFD}" | jq ".stderr" -r
DECOMP_BLAKE3=$(curl -s "${URL}/status/${RFD}" | jq ".checksums.blake3" -r)
DECOMP_XXH128=$(curl -s "${URL}/status/${RFD}" | jq ".checksums.xxh128" -r)

test "${COMP_BLAKE3}" = "${DECOMP_BLAKE3}"
test "${COMP_XXH128}" = "${DECOMP_XXH128}"
