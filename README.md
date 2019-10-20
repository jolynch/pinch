Pinch
=====
A toolkit for rapidly compressing, hashing and otherwise making data smaller
when you can't neccesarily install software (e.g. CI/CD envionments) but you
can run docker containers. The resulting `pinch` container container takes up
about `9MiB` of disk space and contains three excellent compression/hashing
utilities from [Yann Collet](https://github.com/Cyan4973):

* [zstd](https://github.com/facebook/zstd) v1.4.0 for great compression
* [lz4](https://github.com/lz4/lz4) v1.9.1 for very fast compression
* [xxHash](https://github.com/facebook/zstd) v0.7.0 for ridculously fast hashing

Note that licenses of these tools are included in the
`/usr/local/share/licenses` folder within the container.


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
Assuming that you have the pinch wrapper installed:

You can upload a large file to S3 as fast as possible

```
cat large_file | pinch zstd -c --adapt | aws s3 cp - s3://<path>
```
