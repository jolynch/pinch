# Filexfer — Architecture Guide

This document covers the protocol, server, client library, and testing conventions for the filexfer subsystem.

## Directory layout

```
server/filexfer/                   # Public client library
  client.go                        # Client type, options, request/response types
  client_tcp.go                    # TCP transport implementation
  client_test.go                   # Integration tests against a real server
  docs/
    PROTOCOL.md                    # FTCP line protocol (AUTH, TXFER, SEND, ACK, CXSUM, STATUS, PROBE)
    MANIFEST.md                    # FM/1 manifest wire format
    FRAMING.md                     # FX/1 frame wire format
    CLI.md                         # CLI usage reference

server/internal/filexfer/
  ftcp/                            # Server-side FTCP command handlers
    server.go                      # Listener loop and connection dispatch
    verb.go                        # Verb enum
    request.go                     # Protocol line parser (ParseRequest)
    auth.go                        # AUTH handler + age encryption setup
    txfer.go                       # TXFER handler — manifest generation
    send.go                        # SEND handler — file streaming
    ack.go                         # ACK handler — progress acknowledgment
    cxsum.go                       # CXSUM handler — checksum streaming
    status.go                      # STATUS handler — transfer status/list
    probe.go                       # PROBE handler — latency/bandwidth probe
    sync.go                        # SYNC handler
    deps.go                        # Deps interface + runtimeDeps (thin wrapper over store)
    errors.go                      # protocolErr, writeOKLine, writeErrFrame helpers
  encoding/
    manifest.go                    # FM/1 marshal/parse (front-coded paths + mtimes)
    frame.go                       # FX/1 frame marshal/parse
    codec.go                       # Compression codec pools (zstd, lz4, identity)
    format.go                      # Byte/duration string parsing
  store/
    store.go                       # In-memory transfer state (global map, TTL eviction)
  policy/
    policy.go                      # Adaptive compression policy
  limit/
    limit.go                       # Rate-limited io.Writer
  progress.go                      # Background progress-file writer

server/internal/cmd/filexfercli/
  cli.go                           # CLI commands: copy, get, status, verify
  cli_test.go                      # End-to-end CLI tests with fake TCP servers
```

## Protocol

Full specification lives in `docs/PROTOCOL.md`. Key points:

- **Transport**: one TCP connection per command, server closes after completion.
- **Line format**: `VERB args...\r\n` → optional streaming payload → `OK [msg]\r\n` or `ERR <code> <msg>\r\n`.
- **AUTH**: optional first command; supports age encryption for both the command line and response stream.
- **Token encoding**: path/blob args are quoted (`"..."`) or length-prefixed (`<len>:<bytes>`).

### Command sequence for a typical download

```
PROBE cpu=<n> probe-bytes=<n> cts0=<ms>        → measure link
TXFER "<dir>" mode=fast link-mbps=<n> ...       → get FM/1 manifest stream
SEND <tid> fd=0 "<path>" [fd=1 "<path>" ...]    → receive FX/1 frame stream
ACK  <tid> fd=0 "<path>" ack-token=<tok> ...    → confirm windows received
STATUS <tid>                                    → poll progress JSON
STATUS                                          → list all active transfers (count + N JSON lines)
```

## Server internals (`internal/filexfer/`)

### `ftcp/` — command handlers

`Serve()` in `server.go` accepts connections and dispatches on the parsed verb. Each handler receives `(ctx, Request, io.Writer, Deps)`.

**`Deps` interface** (`deps.go`) abstracts all state mutations so handlers are testable without a real store. `runtimeDeps` is a thin pass-through to the `store` package. Tests construct a `mockDeps` implementing the same interface.

**`txfer.go`** handles both directory TXFER (walks the tree with `WalkDir`) and single-file TXFER (`encodeSingleFileManifest`). After writing the manifest it calls `deps.ClipTransfer` to seal the file count.

**`send.go`** is the hot path. It:
1. Opens files through `deps.GetFile` (validates transfer + path).
2. Streams windows as FX/1 frames with per-frame adaptive compression (`policy.CompressionPolicy`).
3. Uses zero-copy sendfile when available, falls back to buffered read.
4. Sets `SetTransferFileWindowHash` so subsequent ACKs can be verified.

**`status.go`** — two modes:
- `STATUS <txferid>`: returns `OK <json>` with a single `TransferStatus`.
- `STATUS` (no args): returns `OK <count>\r\n` followed by `<count>` JSON lines, one per active transfer.

Completed transfers remain in the store until TTL expiry (default 10 min), so `ListTransfers()` returns them.

### `encoding/` — wire formats

- **FM/1 manifest**: header line + one entry line per file. Paths and mtimes use front-coding (delta from previous entry) to compress the manifest without a compression codec.
- **FX/1 frames**: fixed header with file ID, compression name, byte offsets, wire size, and a checksum token. Optional trailer with aggregate metadata.
- **Codec pools**: zstd and lz4 decoder instances are pooled by `sync.Pool` to avoid per-frame allocations.

### `store/` — transfer state

A global `sync.Map`-backed store keyed by random hex transfer ID. `Transfer` holds per-file arrays (`State []uint8`, `FileSize []int64`, `AckedSize []int64`, `PathHash []xxh3.Uint128`). TTL is enforced lazily on reads.

State transitions per file: `Started → Running → Done` (or `Missing` for 404s).

`ClipTransfer` seals the file count after the manifest is written; calls before that can still update `NumFiles`.

### `policy/` — adaptive compression

`CompressionPolicy.Decide(metrics)` returns the next compression mode based on measured read vs. write latency ratios. It upgrades (to zstd) when writes are cheap relative to reads, and downgrades (to lz4 or none) when compression becomes the bottleneck.

## Client library (`filexfer/`)

`Client` is a config struct (not an interface). All public operations are methods on `*Client`:

| Method | Protocol command |
|--------|-----------------|
| `GetManifest` | `TXFER` |
| `GetFiles` | `SEND` + `ACK` |
| `GetStatus` | `STATUS <tid>` |
| `ListStatuses` | `STATUS` (list all) |
| `GetChecksum` | `CXSUM` |
| `ProbeLink` | `PROBE` |
| `SyncManifest` | `SYNC` |
| `StartFromManifest` | orchestrates `SEND` + `ACK` in batches |
| `AcknowledgeFileProgress` | `ACK` |

The `Client` struct holds connection config (`ServerAddr`, `ServerAgePublicKey`) and client-side encryption keys (`ClientAgePublicKey`, `ClientAgeIdentity`). Age keys on the client struct are used automatically by all methods that need encryption, so request structs do not carry them.

TCP helpers live in `client_tcp.go`. Each method dials a fresh connection (no persistent connection pool). `readTCPStatus` reads the `OK`/`ERR` terminal line; `readTCPLine` reads an arbitrary line up to `maxTCPLineBytes`.

`TransferStatus` in `client.go` mirrors the JSON schema returned by the server's STATUS command.

## CLI (`cmd/filexfercli/`)

Three user-facing commands defined in `cli.go`:

- **`copy`**: full directory download with probes, manifest fetch, parallel SEND batches, ACK, optional verify. Writes `.pinch/` state (manifest + progress file) for resume.
- **`get`**: single-file download. Skips probes and `.pinch/` state.
- **`status`**: monitors a transfer. With `LOCAL_DST` reads `.pinch/manifest.server` for the transfer ID and polls with combined server+client progress. With `--tid` polls server only. With no args lists all active transfers.

## Testing

### Unit tests (per package)

Each `ftcp/` handler has a `*_test.go` with a `mockDeps` that implements `Deps`. Tests construct a fake `Request`, pass a `bytes.Buffer` as the writer, and assert the response bytes.

`request_test.go` covers the protocol parser extensively including edge cases (quoted paths, length-prefixed blobs, missing args).

### End-to-end CLI tests (`cli_test.go`)

`TestRunCLITransferAndGet` and friends spin up a real `net.Listener` in-process, implement a minimal FTCP server responding to PROBE / TXFER / SEND / ACK, and invoke `RunCLI` against it. This validates the full client+CLI stack without requiring real files.

Pattern for a test server:
```go
ln, _ := net.Listen("tcp", "127.0.0.1:0")
go func() {
    conn, _ := ln.Accept()
    // read/write raw FTCP protocol
}()
RunCLI([]string{"copy", "--server", ln.Addr().String(), ...})
```

When extending `Deps`, add the new method to `mockDeps` in `cli_test.go` (and any other test files that define their own mock) before running `go test ./...`.

### Running tests

```sh
go test ./server/...                     # all packages
go test ./server/internal/filexfer/...  # server packages only
go test -run TestRunCLI ./server/internal/cmd/filexfercli/  # CLI tests
go test -bench=. ./server/internal/filexfer/encoding/       # codec benchmarks
```
