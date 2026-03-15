pipetee
=======

Why a faster tee? Because the built-in tee in busybox does one read syscall
and O(n) write syscalls per 1024 bytes of input. This is ... extremely
slow and we could be using zero copy.

This version uses the Linux specific zero copy syscalls:
* [`tee`](https://man7.org/linux/man-pages/man2/tee.2.html): To zero copy data
  between kernel buffers (pipes).
* [`splice`](https://man7.org/linux/man-pages/man2/splice.2.html): To zero
  copy data between the programs internal buffers and the destination file
  descriptors.

This technique:
1. Does not copy data into userspace at all, instead just zero copying it
   around in the kernel.
2. Does about 64KiB of work per syscall instead of 1KiB

It is based on the version of `tee.c` from the manpage for `man 2 tee`
except it:
* supports multiple output files
* supports inputs and outputs that are regular files (the version from the
  man page only supports pipes on stdin and stdout).
* doesn't loop infinitely when the downstream pipe is slow
  (instead it blocks for the downstream to take the data)

## Examples
```bash
### Baseline

# Baseline performance
$ pv -s 50g -S -teba /dev/zero | cat > /dev/null
50.0GiB 0:00:04 [11.4GiB/s]

### tee

# Standard tee, just adding it makes you slower
$ pv -s 50g -S -teba /dev/zero | tee /dev/null | cat > /dev/null
50.0GiB 0:00:08 [6.17GiB/s] 
# Standard tee, doing anything at all makes you _much_ slower - Ouch
$ pv -s 50g -S -teba /dev/zero | tee >(wc -c) | cat > /dev/null
50.0GiB 0:00:14 [3.50GiB/s]  

### pipetee

# Now pipetee to the rescue (strangely faster than no tee) 
$ pv -s 50g -S -teba /dev/zero | pipetee /dev/null | cat > /dev/null
[pipetee] pipe input, buffers of size 65536, 1 outputs
50.0GiB 0:00:04 [12.4GiB/s]
# And now you can do things to your data without slowing things down too much
$ pv -s 50g -S -teba /dev/zero | pipetee >(wc -c) | cat > /dev/null
[pipetee] pipe input, buffers of size 65536, 1 outputs
50.0GiB 0:00:04 [10.1GiB/s] 
# And we can even size the pipes up to recover some speed for a little memory
$ pv -s 50g -S -teba /dev/zero | pipetee -b 131072 >(wc -c) | cat > /dev/null
[pipetee] pipe input, buffers of size 131072, 1 outputs
50.0GiB 0:00:04 [11.9GiB/s]
# Supercharge with more memory
pv -s 50g -S -teba /dev/zero | pipetee/pipetee -b $(cat /proc/sys/fs/pipe-max-size) /dev/null | cat > /dev/null
[pipetee] pipe input, buffers of size 1048576, 1 outputs
50.0GiB 0:00:03 [16.3GiB/s]
```

## Usage notes - Important difference from `tee`

**stdout must be a pipe.** `pipetee` uses `splice(2)` to pass data through to
stdout, which requires stdout to be a pipe. Writing directly to a terminal will
fail with `Invalid argument`. If you really need stdout, redirect to
`/dev/stdout` to get a pipe-backed fd that works in your terminal:

```bash
echo "hello world" | pipetee /tmp/wat /tmp/bar >(wc) > /dev/stdout
# [pipetee] pipe input, buffers of size 65536, 3 outputs
# hello world
#       1       2      12
```

## Options

```
Usage: pipetee/pipetee [OPTION] [FILE]...
  -a    Append to files instead of truncating.
  -b    Pipe buffer sizes in bytes. Defaults to the size of the input pipe or 131072 for files.
```

### `-a` — append mode

By default `pipetee` truncates existing regular files before writing, matching
the behavior of `> file` redirections. Use `-a` to append instead, matching
`tee -a`. This has no effect on pipes or FIFOs.

```bash
# First run writes "hello world"
echo "hello world" | pipetee /tmp/log > /dev/null
# Second run appends "goodbye" rather than overwriting
echo "goodbye" | pipetee -a /tmp/log > /dev/null
```

### `-b BYTES` — pipe buffer size

Each internal kernel buffer pipe is sized to match the input pipe (default
65536 bytes on Linux). Larger buffers mean fewer syscalls and higher
throughput at the cost of memory. The maximum is capped at 1 MiB; use
`/proc/sys/fs/pipe-max-size` to find the system limit.

```bash
# Use the system maximum pipe buffer size for peak throughput
pv -s 50g -S /dev/zero | pipetee -b $(cat /proc/sys/fs/pipe-max-size) /dev/null | cat > /dev/null
# [pipetee] pipe input, buffers of size 1048576, 1 outputs
# 50.0GiB 0:00:03 [16.3GiB/s]
```
