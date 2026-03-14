# FTCP Protocol (`-file-listen`)

This document defines the TCP file-transfer command protocol implemented by Pinch.

## Transport

- Listener: `-file-listen` (for example `127.0.0.1:3453`)
- One connection serves at most one command (optionally preceded by `AUTH`)
- Server closes the connection after command completion (or on error)

## Line Protocol

All commands are single lines terminated by `\r\n`.

- Request line: `<VERB> <args...>\r\n`
- Optional command-specific payload bytes (depends on command semantics)
- Response status line:
  - `OK\r\n`
  - `OK <message>\r\n`
  - `ERR <code> <message>\r\n`

For `TXFER`, `SEND`, and `CXSUM`, the payload interval is a streaming body
(`FM/2` for `TXFER`, `FX/1` for `SEND`/`CXSUM`) between the request line and
the terminal response status line. `PROBE` also has a request payload and
response payload body.

Maximum command line size is 4 MiB.

## Connection Flow

1. Client connects.
2. Client sends either:
   - command line (`TXFER|SEND|ACK|CXSUM|STATUS|PROBE`), or
   - `AUTH` first, then exactly one command line.
3. Server writes response.
4. Server closes connection.

If `-fs-require-auth=true`, first line must be `AUTH`.

## Token Encoding

Most args are plain space-delimited tokens.

Path/blob arguments use one of:

- quoted text: `"..."` (supports escapes like `\"` and `\\`)
- length-prefixed bytes: `<len>:<bytes>`

For this line protocol, token bytes cannot span command newlines.

## AUTH

### Request

- `AUTH`
- `AUTH <blob>`

`<blob>` can be quoted or length-prefixed.

### Behavior

When `-fs-require-auth=true`:

- blob must decode to age ciphertext decryptable by server identity.
- decrypted plaintext must be client age recipient string.
- if valid:
  - subsequent response bytes are age-encrypted to client recipient.
  - subsequent command line must be age-encrypted to server identity.
- if invalid: `ERR NOT_AUTHORIZED authorization failed`.

When `-fs-require-auth=false`:

- empty `AUTH` is accepted (no encryption).
- non-empty blob must be a plaintext age recipient string.
- server encrypts responses to that recipient.
- command line remains plaintext.

## TXFER

Creates a transfer and streams a manifest.

### Request

`TXFER <path> mode=<fast|gentle> link-mbps=<int> concurrency=<int> [verbose=<0|1|true|false>] [max-manifest-chunk-size=<n>]`

- `<path>` must be quoted or length-prefixed.
- directory must be absolute, existing, and readable.
- `mode`, `link-mbps`, and `concurrency` are required.
- `link-mbps` must be `>= 0`.
- `concurrency` must be `> 0`.

### Response

- Stream bytes in `FM/2` format (see [MANIFEST.md](./MANIFEST.md)).
- Terminal status line after manifest stream: `OK` or `ERR ...`.

## SEND

Streams one or more file windows as `FX/1` frames.

### Request

`SEND <txferid> fd=<fid> <path> [offset=<n>] [size=<n>] [comp=<name>] [mode=<fast|gentle>] [<unknown key=value>...] [fd=<fid> <path> ...]`

- each `fd=` starts a new file block.
- required per block: `fd`, `path`.
- `offset` defaults to `0`.
- `size` defaults to `0` (means "from offset to EOF").
- `comp` defaults to `adapt`.
- `mode` defaults to `fast`.
- accepted compression values: `adapt`, `none`, `identity`, `lz4`, `zstd`.
- accepted load strategy values: `fast`, `gentle`.
- `identity` is normalized to `none`.
- in `adapt`, server may emit different per-frame `comp` values as it adjusts compression.
- unknown compression values are rejected with `ERR UNSUPPORTED_COMP ...`.
- each `<path>` is quoted or length-prefixed.
- unknown `key=value` fields are ignored.

### Response

- Continuous `FX/1` stream for all tuples, in request order (see [FRAMING.md](./FRAMING.md)).
- Terminal status line after stream: `OK` or `ERR ...`.

## ACK

Acknowledges file progress/window completion.

### Request

`ACK <txferid> fd=<fid> <path> ack-token=<token> [delta-bytes=<n>] [recv-ms=<n>] [sync-ms=<n>] [<unknown key=value>...] [fd=<fid> <path> ...]`

- each `fd=` starts a new ack block.
- required per block: `fd`, `path`, `ack-token`.
- telemetry fields default to `0` when omitted.
- unknown `key=value` fields are ignored.

`ack-token` forms:

- missing file: `-1`
- positive progress: `<ack-bytes>@<server-ts>@<hash-token>`

Rules:

- non-`-1` ack must include hash token.
- hash is validated against stored window hash at ack offset.

### Response

- `OK` / `OK <message>` / `ERR ...`

## CXSUM

Streams checksum frames for a file.

### Request

`CXSUM <txferid> <fid> <window-size> <checksums-csv> <path>`

- `<path>` is quoted or length-prefixed.
- algorithms: `xxh128`, `xxh64`, `none`.

### Response

- `FX/1` frame stream (rolling checksums)
- terminal status line: `OK` or `ERR ...`

## STATUS

Returns transfer status JSON.

### Request

`STATUS <txferid>`

### Response

- success: `OK <json>`
- failure: `ERR <code> <message>`

JSON schema:

```json
{
  "transfer_id": "string",
  "directory": "string",
  "num_files": 0,
  "total_size": 0,
  "done": 0,
  "done_size": 0,
  "percent_files": 0,
  "percent_bytes": 0,
  "download_status": {
    "started": 0,
    "running": 0,
    "done": 0,
    "missing": 0
  }
}
```

## PROBE

Latency/throughput probe used before `TXFER` so the client can send transfer hints.

### Request

`PROBE cpu=<client-cpu> probe-bytes=<n> cts0=<unix-ms>`

- request line is followed by exactly `probe-bytes` raw bytes.
- `probe-bytes` must be `<= 32 MiB`.

### Response

- first line:
  - `PROBE cpu=<server-cpu> cts0=<echo-client-cts0> sts0=<unix-ms> sts1=<unix-ms> probe-bytes=<n>`
- then exactly `probe-bytes` raw bytes.
- terminal status line: `OK` or `ERR ...`.

Clients typically run 3 probes, compute a rounded link estimate, choose mode/concurrency, then issue `TXFER` with those required hints.
