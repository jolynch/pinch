pinch-server
============
The pinch server provides UNIX pipes and streaming HTTP endpoints for
compressing and decompressing data either via a local sidecar (read and write
to well defined files) or HTTP (read and write to a streaming HTTP interface)

Example bi-directional compression and decompression:
```
# Setup the pinch and unpinch pipelines, read out the handles
FD=$(curl -s 'localhost:8080/pinch?min_level=0&timeout=200' | jq '.handles[0]' -r);
RFD=$(curl -s 'localhost:8080/unpinch?timeout=200' | jq '.handles[0]' -r);

echo "Read at localhost:8080/read/${FD}";

# Round trip compress and decompress
curl -s -T yelp_academic_dataset_review.json -v "localhost:8080/write/${FD}?c&rw" | curl -svT - "localhost:8080/write/${RFD}?c&rw" | wc -c
```
