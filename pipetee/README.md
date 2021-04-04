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
