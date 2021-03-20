# An image that contains all three of Yann Collet's data compression tools.
# Based off of https://github.com/facebook/zstd/issues/880

# Step 1. Image used to build the binary
FROM alpine:3.10.1 as builder

RUN apk --no-cache add make gcc libc-dev git

# Only git clone what we need
RUN git clone --depth=1 --branch v1.9.3 https://github.com/lz4/lz4.git         /lz4_src
RUN git clone --depth=1 --branch v1.4.9 https://github.com/facebook/zstd.git   /zstd_src
RUN git clone --depth=1 --branch v0.8.0 https://github.com/Cyan4973/xxHash.git /xxh_src

RUN mkdir /build_lz4  && cd /lz4_src  && make -j 8 && make DESTDIR=/build_lz4  install
RUN mkdir /build_zstd && cd /zstd_src && make -j 8 && make DESTDIR=/build_zstd install
RUN mkdir /build_xxh  && cd /xxh_src  && make -j 8 && make DESTDIR=/build_xxh  install

RUN strip /build_lz4/usr/local/bin/lz4
RUN strip /build_zstd/usr/local/bin/zstd
RUN strip /build_xxh/usr/local/bin/xxhsum

# We don't need libraries or headers
RUN rm -rf /build_lz4/usr/local/include/*
RUN rm -rf /build_lz4/usr/local/lib/*
RUN rm -rf /build_zstd/usr/local/include/*
RUN rm -rf /build_zstd/usr/local/lib/*
RUN rm -rf /build_xxh/usr/local/include/*
RUN rm -rf /build_xxh/usr/local/lib/*

# Native binaries are nice, just b3sum native binary and strip that too
RUN wget https://github.com/BLAKE3-team/BLAKE3/releases/download/0.3.7/b3sum_linux_x64_bin -O /usr/local/bin/b3sum
RUN chmod +x /usr/local/bin/b3sum
RUN strip /usr/local/bin/b3sum

# Step 2. Minimal image to only keep the built binaries, this should be about 11MiB
FROM alpine:3.10.1

RUN apk --no-cache add man bash
RUN apk --no-cache del openssl

COPY --from=builder /build_lz4  /
COPY --from=builder /build_zstd /
COPY --from=builder /build_xxh  /
COPY --from=builder /usr/local/bin/b3sum /usr/local/bin/b3sum

# For some reason man pages don't work unless in the global setup
RUN ln -sf /usr/local/share/man/man1 /usr/share/man/man1

# Make sure to include the licenses
RUN mkdir -p /usr/local/share/licenses/lz4 /usr/local/share/licenses/zstd /usr/local/share/licenses/xxhash
COPY --from=builder /lz4_src/LICENSE /usr/local/share/licenses/lz4/
COPY --from=builder /zstd_src/LICENSE /usr/local/share/licenses/zstd/
COPY --from=builder /xxh_src/LICENSE /usr/local/share/licenses/xxhash/


ENV PAGER less

# Zstd is probably the right default choice
CMD ["/usr/local/bin/zstd"]
