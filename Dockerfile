# An image that contains all three of Yann Collet's data compression tools.
# Based off of https://github.com/facebook/zstd/issues/880

# Step 1. Image used to build the binary
FROM alpine:3.10.1 as builder

RUN apk --no-cache add make gcc libc-dev git

# Only git clone what we need
RUN git clone --depth=1 --branch v1.9.1 https://github.com/lz4/lz4.git         /lz4_src
RUN git clone --depth=1 --branch v1.4.0 https://github.com/facebook/zstd.git   /zstd_src
RUN git clone --depth=1 --branch v0.7.0 https://github.com/Cyan4973/xxHash.git /xxh_src

RUN mkdir /build_lz4  && cd /lz4_src  && make -j 8 && make DESTDIR=/build_lz4  install
RUN mkdir /build_zstd && cd /zstd_src && make -j 8 && make DESTDIR=/build_zstd install
RUN mkdir /build_xxh  && cd /xxh_src  && make -j 8 && make DESTDIR=/build_xxh  install

# Step 2. Minimal image to only keep the built binaries, this should be about 9MiB
FROM alpine:3.10.1

COPY --from=builder /build_lz4  /
COPY --from=builder /build_zstd /
COPY --from=builder /build_xxh  /

# Make sure to include the licenses
RUN mkdir -p /usr/local/share/licenses/lz4
COPY --from=builder /lz4_src/LICENSE /usr/local/share/licenses/lz4/

RUN mkdir -p /usr/local/share/licenses/zstd
COPY --from=builder /zstd_src/LICENSE /usr/local/share/licenses/zstd/

RUN mkdir -p /usr/local/share/licenses/xxhash
COPY --from=builder /xxh_src/LICENSE /usr/local/share/licenses/xxhash/

# Zstd is probably the right default choice
CMD ["/usr/local/bin/zstd"]
