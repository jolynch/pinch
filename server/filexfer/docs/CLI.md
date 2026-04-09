# File Transfer CLI

This document describes the current `pinch filecli` command line, the
high-level copy workflow, and the `fast` / `gentle` transfer strategies used by
the transfer layer.

## Overview

The file transfer CLI currently exposes three public commands:

- `copy`: copy a remote tree to a local destination
- `status`: query transfer status by transfer id or local destination
- `get`: download a single remote file by absolute path

Top-level usage:

```text
pinch filecli [<addr>] <command> [options]
```

- `<addr>` defaults to `127.0.0.1:3453`
- state is stored in `<LOCAL_DST>/../.pinch/`

The state directory contains:

- `manifest.server`: the last remote manifest snapshot
- `manifest`: the local manifest after a successful write
- `manifest.progress`: resumable progress state
- `remote/`: start-phase staging directory

## `copy`

`copy` is the main entry point.

```text
pinch filecli [<addr>] copy [flags] REMOTE_SRC LOCAL_DST
```

Behavior:

- if `LOCAL_DST` does not exist, `copy` performs a full transfer into a staging
  directory and then renames it into place
- if `LOCAL_DST` already exists, `copy` switches to sync mode and applies the
  delta needed to converge the local tree to the remote tree
- successful non-`--skip-fetch` runs clean up `.pinch` after they finish

Common examples:

```bash
pinch filecli copy /srv/data /var/lib/pinch/data
pinch filecli copy --clean /srv/data /var/lib/pinch/data
pinch filecli copy --mode gentle /srv/data /var/lib/pinch/data
pinch filecli copy --verify-meta /srv/data /var/lib/pinch/data
pinch filecli copy --verify-data-sample 5 /srv/data /var/lib/pinch/data
pinch filecli copy --deadline 30m /srv/data /var/lib/pinch/data
```

Important flags:

- `--mode fast|gentle`: select the transfer strategy (default: fast)
- `--clean`: remove `LOCAL_DST` first, then force a clean transfer
- `--skip-fetch`: only refresh manifest state; do not write file contents
- `--skip-write`: fetch data to a discard sink and avoid mutating `LOCAL_DST`
- `--skip-fsync`: acknowledge writes without `fdatasync`
- `--verify-meta`: run a read-only follow-up verification pass
- `--verify-data-sample N`: sample file contents after a successful copy;
  implies `--verify-meta`
- `--encrypt none|auto|aes|chacha20`: encryption algorithm (default: none)
- `--compress adapt|none|lz4|zstd`: compression algorithm (default: adapt)
- `--concurrency N`: parallel download / verification workers (0=auto)
- `--deadline`: cap a run to a fixed duration
- `-a`, `--ack-every`: bytes between progress acks (e.g. `1B`, `4KiB`, `8MiB`)
- `--probe-size`: probe payload size (e.g. `1B`, `4KiB`, `8MiB`)
- `-y`, `--yes`: skip confirmation prompt on sync paths
- `--progress`, `-v`/`--verbose`, `--progress-file`, `--progress-file-interval`:
  control human and file-based progress output
- `--trace`: write `runtime/trace` output to a file

## Convergence Workflow

`copy` is designed so you can rerun the same command until the local tree is
fully consistent with the remote tree.

Typical pattern:

1. Run a bounded first pass:

   ```bash
   pinch filecli copy --deadline 30m --mode gentle /srv/data /var/lib/pinch/data
   ```

2. Run the same command again in fast mode to get deltas:

   ```bash
   pinch filecli copy /srv/data /var/lib/pinch/data
   ```

3. Keep rerunning until the sync phase reports:

   ```text
   sync: remote and local converged, nothing to do
   ```

Why this works:

- the first run creates `LOCAL_DST`
- later runs see an existing destination and take the sync path instead of the
  clean start path
- sync only downloads new or stale files and removes remote-missing files when
  needed

If you want an explicit final check, add `--verify-meta` to the last run.

## `status`

`status` queries transfer progress. It supports three modes:

```text
pinch filecli [<addr>] status [--tid <id>] [LOCAL_DST]
```

- **With `LOCAL_DST`**: reads `.pinch/manifest.server` for the transfer ID and
  polls with combined server + client progress.
- **With `--tid <id>`**: polls server-side status only for that transfer.
- **With no arguments**: lists all active transfers on the server.

Examples:

```bash
pinch filecli status /var/lib/pinch/data
pinch filecli status --tid bc17bc4e
pinch filecli status
```

## `get`

`get` downloads a single remote file by its absolute server path.

```text
pinch filecli [<addr>] get [flags] REMOTE_PATH
```

- `REMOTE_PATH` must be an absolute path to a file on the server.
- Output defaults to the file's basename in the current directory.

Examples:

```bash
pinch filecli get /srv/data/file.bin
pinch filecli get -o /tmp/out.bin /srv/data/file.bin
pinch filecli get -o - /srv/data/file.bin          # write to stdout
pinch filecli get --skip-write /srv/data/file.bin   # fetch without writing
```

Important flags:

- `-o <path|->`: output file path, or `-` for stdout
- `--encrypt none|auto|aes|chacha20`: encryption algorithm (default: none)
- `--compress adapt|none|lz4|zstd`: compression algorithm (default: adapt)
- `--concurrency N`: parallel download workers (0=auto)
- `--skip-write`: fetch to discard without writing
- `--skip-fsync`: acknowledge writes without `fdatasync`
- `-a`, `--ack-every`: bytes between progress acks (e.g. `1B`, `4KiB`, `8MiB`)
- `--deadline`: transfer deadline (e.g. 60s, 5m)
- `--progress`, `-v`/`--verbose`, `--progress-file`, `--progress-file-interval`:
  control human and file-based progress output
- `--trace`: write `runtime/trace` output to a file

## Transfer Strategies: `fast` and `gentle`

The transfer layer supports two load strategies:

- `fast`: maximize throughput and finish as quickly as possible
- `gentle`: reduce pressure on the source side and trade peak throughput for
  lower impact

Use `fast` when:

- the source host is dedicated to the transfer
- you want the shortest wall-clock time
- transient read or CPU pressure is acceptable

Use `gentle` when:

- the source host is shared with other work
- you want a lower-impact first pass
- you expect to converge over multiple runs instead of finishing in one shot

Both `copy` (`--mode fast|gentle`) and the lower-level transfer phase support
strategy selection.

## Notes

- `REMOTE_SRC` must be an absolute path on the remote server
- `LOCAL_DST` is local filesystem state on the client machine
- human-readable byte sizes are accepted for flags such as:
  - `--ack-every`
  - `--probe-size`
- a successful sync prompt is skipped automatically when no local mutations are
  needed
