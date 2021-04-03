#!/bin/bash
set -euo pipefail
set -x

./pinch zstd -f -d tests/state/webster.txt.zst

echo "94331ecc0e8037e1e41b01e3081429f4c072fb50080b9415f97f48c87cc123be  tests/state/webster.txt" | ./pinch b3sum --check -
echo "ba35b1b4693a3d1f7c4b085629f968e8  tests/state/webster.txt" | ./pinch xxh128sum --check -

