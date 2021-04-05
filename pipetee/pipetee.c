#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>

void usage (char *argv[]) {
    fprintf(stderr, "Usage: %s [OPTION] [FILE]...\n", argv[0]);
    fprintf(stderr, "  -b    Pipe buffer sizes in bytes. Defaults to the size of the input pipe or 131072 for files.\n");
    exit(EXIT_FAILURE);
}

long buffer_size(long user_size, long default_size) {
    if (user_size > 0 && user_size <= 1048576) {
        return user_size;
    }
    return default_size;
}

int main(int argc, char *argv[]) {
    int len, dlen, slen, nfd, ret;
    long buf_size = 0;
    long inpipe_size = 0;
    int opt;

    while ((opt = getopt(argc, argv, "b:")) != -1 ) {
        switch (opt) {
        case 'b':
            buf_size = atoi(optarg);
            break;
        default:
            usage(argv);
            exit(EXIT_FAILURE);
        }
    }

    nfd = (argc - optind);
    if (nfd < 1) {
        usage(argv);
        exit(EXIT_FAILURE);
    }
    int fds[nfd];

    // How large is our input pipe, we will need each buffer pipe
    // to be at least as large.
    inpipe_size = (long)fcntl(STDIN_FILENO, F_GETPIPE_SZ);
    if (inpipe_size < 0) {
        buf_size = buffer_size(buf_size, 128 * 1024);
        fprintf(stderr, "[pipetee] file input, buffers of size %ld, %d outputs\n", buf_size, nfd);
    } else {
        buf_size = buffer_size(buf_size, inpipe_size);
        if (buf_size > inpipe_size) {
            fcntl(STDIN_FILENO, F_SETPIPE_SZ, buf_size);
            fcntl(STDOUT_FILENO, F_SETPIPE_SZ, buf_size);
            inpipe_size = (long)fcntl(STDIN_FILENO, F_GETPIPE_SZ);
            if (inpipe_size > 0) {
                buf_size = inpipe_size;
            }
        }
        fprintf(stderr, "[pipetee] pipe input, buffers of size %ld, %d outputs\n", buf_size, nfd);
    }

    /*
     * The tee syscall can only duplicate to pipes, so we need to have
     * a kernel "buffer" (aka pipe) we control per output file and
     * one for our output. Without this approach we can't know that our
     * destinatios can receive the full copy in one tee call.
     */
    int buffers[nfd+1][2];

    for (int i = 0; i < nfd; i++) {
        fds[i] = open(argv[i+optind], O_WRONLY | O_CREAT, 0644);
        if (fds[i] < 0) {
            fprintf(stderr, "Could not open %s\n", argv[i+optind]);
            perror("open");
            exit(EXIT_FAILURE);
        }
        if (pipe(buffers[i]) < 0) {
            perror("buffer");
            exit(EXIT_FAILURE);
        }
        // Since we'll be teeing to these we need these to work
        ret = fcntl(buffers[i][0], F_SETPIPE_SZ, buf_size);
        if (ret < 0) {
            perror("setpipe_sz");
            exit(EXIT_FAILURE);
        }
        // If we are producing to a pipe (e.g. a >( cmd )) try raising tha
        // pipe size there too
        fcntl(fds[i], F_SETPIPE_SZ, buf_size);
    }
    // Last pipe is our input buffer
    if (pipe(buffers[nfd]) < 0) {
        perror("inbuf");
        exit(EXIT_FAILURE);
    } else {
        ret = fcntl(buffers[nfd][0], F_SETPIPE_SZ, buf_size);
        if (ret < 0) {
            perror("setpipe_sz");
            exit(EXIT_FAILURE);
        }
    }

    while (1) {
        /*
         * (1) Zero copy as many bytes as we can from the input FD to our
         * buffer pipe but don't exceed the size of our downstream buffers
         * (as we need the tee calls below to always succeed in one call)
         */
        len = splice(STDIN_FILENO, NULL, buffers[nfd][1], NULL, buf_size, SPLICE_F_MOVE);

        if (len < 0) {
            perror("stdin_splice");
            exit(EXIT_FAILURE);
        } else if (len == 0) {
            // No more input, time to go
            break;
        }

        /*
         * (2) Now we tee the same number of bytes to each of our output
         * pipes (zero-copy) followed by splicing those output pipes into our
         * output fds (zero-copy).
         *
         * Note that we are potentially blocking for the downstreams to consume
         * our data stream here, don't pipetee to slow programs ...
         */
        for (int i = 0; i < nfd; i++) {
            // "Copy" our input pipe to each output buffer using tee (zero copy)
            dlen = tee(buffers[nfd][0], buffers[i][1], len, 0);
            if (dlen != len) {
                // Since we can't "re-copy" this is a failure condition
                fprintf(stderr, "Not able to do a full tee zero-copy! %d %d\n", slen, len);
                exit(EXIT_FAILURE);
            }
            // Drain the "copied" buffer to the downstream FDs
            while (dlen > 0) {
                slen = splice(buffers[i][0], NULL, fds[i], NULL,
                              dlen, SPLICE_F_MOVE);
                if (slen < 0) {
                    fprintf(stderr, "Cannot splice to tee output, is it a pipe or regular file?\n");
                    perror("splice");
                    exit(EXIT_FAILURE);
                }
                dlen -= slen;
            }
        }

        /*
         * Finally we splice from our buffer to stdout
         */
        while (len > 0) {
            slen = splice(buffers[nfd][0], NULL, STDOUT_FILENO, NULL, len, SPLICE_F_MOVE);
            if (slen < 0) {
                fprintf(stderr, "Cannot splice to output, is it a pipe or regular file?\n");
                perror("splice");
                exit(EXIT_FAILURE);
            }
            len -= slen;
        }
    }

    for (int i = 0; i < nfd; i++) {
        close(fds[i]);
        close(buffers[i][0]);
        close(buffers[i][1]);
    }
    close(buffers[nfd][0]);
    close(buffers[nfd][1]);
    exit(EXIT_SUCCESS);
}
