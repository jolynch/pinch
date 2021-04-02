#!/bin/bash
set -eou pipefail
set -x

if [ ! -f tests/state/webster.txt ]; then
    echo "Downloading Silesia webster data"
    curl http://sun.aei.polsl.pl/~sdeor/corpus/webster.bz2 | bzip2 -d -c - > tests/state/webster.txt
else
    echo "Silesia dataset already exists"
fi
