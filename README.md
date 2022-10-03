Pinch
=====
Associated blog post: [Use Fast Data Algorithms](https://jolynch.github.io/posts/use_fast_data_algorithms/).

A toolkit for rapidly compressing, hashing and otherwise making data smaller
when you can't neccesarily install software (e.g. CI/CD envionments) but you
can run docker containers. The resulting `pinch` container container takes up
about `15MiB` of disk space and contains multiple excellent compression/hashing
utilities from [Yann Collet](https://github.com/Cyan4973) in addition to a few
other tools.

* [zstd](https://github.com/facebook/zstd) v1.4.9 for great compression
* [lz4](https://github.com/lz4/lz4) v1.9.3 for very fast compression
* [xxHash](https://github.com/facebook/zstd) v0.8.0 for ridiculously fast hashing
* [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) v0.3.7 for ridiculously fast cryptographic hashing
* [age](https://github.com/FiloSottile/age) v0.3.7 for reasonably fast encryption/decryption

Note that licenses of these tools are included in the
`/usr/local/share/licenses` folder within the container except for blake3 which
is released to the [public domain](https://github.com/BLAKE3-team/BLAKE3/blob/master/LICENSE).


Installation
------------

You can build the docker image (or grab it from the hub) with
```
docker build -t pinch .
```
At this point you can run the included `pinch` script.

Or you can have the makefile built the container and install the `pinch` wrapper
locally:

```
$ make
$ make PREFIX=~/.local install
```

Running
-------
Use the included `pinch` wrapper script to run one of the included tools on
some files. Pass the tool you want as an argument, for example:

```
# For excellent data compression
$ pinch zstd --help

# For fast data compression
$ pinzh lz4 --help

# For data hashing/validation
$ pinch xxhsum --help

# For crypto data hashing/validation
$ pinch b3sum --help

# For enc/dec files
$ pinch age --help
```

Compression (zstd and lz4)
--------------------------
`pinch` bundles a few compression tools for quick access.

`zstd` is almost always the correct choice for _good_ compression. In my
experience it uses significantly less compute resources (and
compresses/decompresses faster) than similarly "good" algorithms like `gzip` or
`xz`. Of particular interest is this container includes a version of `zstd`
with the `--adapt` functionality included (see the [v1.3.6 release
notes](https://github.com/facebook/zstd/releases/tag/v1.3.6)). This means that
you can do streaming uploads and downloads of massive datasets while your
compression algorithm adaptively adjusts to the bandwidth and CPU environment
for maximum performance.

`lz4` is almost always the correct choice for _fast_ compression. In my
experience it achieves similar ratios as other "fast" algorithms like `snappy`
but uses less compute.

Hashing (xxHash)
----------------

For data verification, there isn't usually a faster choice than taking the
`xxHash` of your data

Advanced Examples
-----------------
Some of the things you can do:

You can upload a large file to S3 as fast as possible

```
$ cat large_file | pinch zstd -c --adapt | aws s3 cp - s3://<path>
```

See what kind of awesome compression ratio you'll get at level 10
```
wc -c large_file
cat large_file | pinch zstd -c -10 | wc -c
```

Read a man page of one of the tools
```
$ pinch man zstd
$ pinch man xxh64sum
$ pinch man lz4
```
