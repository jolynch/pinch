# Filexfer Framing Specification (Draft v1)

This document defines the wire framing for fast efficient
file transfers over plain TCP sockets. Note that while
we may choose to use HTTP to wrap this framing format, this
allows us to manipulate the compression and hashing as we
go, in a way chunked encoding does not. It also allows for
the client to initiate multiple round trips which retrieve
say 128MiB at a time and then acknowledge as it is writing
that data to disk, preventing re-work.

## Scope

- Transport: plain TCP stream.
- This framing is for per-file transfer messages after a manifest has been exchanged out of band.
- `file_id` is an integer index into that exchanged manifest (0-based).

## Frame Structure

Each file transfer response frame is:

1. Header line (ASCII, UTF-8 safe)
2. Raw payload bytes
3. Trailer line (ASCII, UTF-8 safe)

Header and trailer lines are terminated by `\n`.

### First Header Line

Format:

```text
FX/1 <file_id> <properties...>
```

- `FX/1`: protocol version token.
- `<file_id>`: manifest-relative file ID (unsigned integer).
- `<properties...>`: space-separated `property=value` tokens.

Example:

```text
FX/1 12 offset=0 size=1048576 wsize=262144 comp=zstd enc=age hash=xxh128:9f12ab... deadline:30s
<262144 payload bytes>
FXT/1 12 status=ok hash=xxh64:1af3bc9d00ee42aa
```

## Properties

Properties are ASCII and case-sensitive.

### Required

- `comp=<mode>`: compression mode.
- `enc=<mode>`: encryption mode.
- `offset=<n>`: byte offset within the logical file where payload data belongs.
- `size=<n>`: number of original (logical, uncompressed) bytes represented by this frame.
- `wsize=<n>`: number of payload bytes on the wire for this frame.
- `ts=<unix_ms>`: server timestamp (unix milliseconds) when this frame header is emitted.

### Optional

- `hash=<algo>:<value>`: checksum token carried on frame headers/trailers.
  - Current trailer frame checksum algorithm is `xxh64`.
  - Header `hash` is backward-compatible metadata from transfer emission flow.
  - At most one `hash` token per frame.
- `max-wsize=<bytes>`: server hint for maximum wire payload bytes per frame for this response window.
  - Emitted on the first `/fs/file` frame.
  - Current bucket algorithm is ceiling in `{1,2,4,8,16,32,64} MiB`.
- `deadline:<duration>`: per-frame deadline using Go-style duration syntax (for example `30s`, `2m`, `500ms`).

## Compression

`comp` allowed values:

- `none`
- `zstd`
- `lz4`

Receiver behavior:

- `none`: write bytes directly.
- `zstd` or `lz4`: decompress before writing to destination offset.

## Encryption

`enc` allowed values:

- `none`
- `age`

Receiver behavior:

- `none`: interpret payload as plaintext/compressed bytes per `comp`.
- `age`: decrypt payload before decompression and write.

## Parsing Rules

- Maximum header bytes: 16 KiB (defensive limit).
- Unknown properties are ignored.
- Missing required fields (`comp`, `enc`, `offset`, `size`, `wsize`) reject frame.
- Invalid `file_id` reject frame.
- Invalid numeric value formats reject frame.
- Invalid `deadline` duration format rejects frame when `deadline:` is present.
- Header must be exactly one line; no multi-line property blocks.

## Semantics

- `mtime`, mode/permissions, and full-file `size` are manifest properties, not framing properties.
- `offset` allows resumable/partial writes.
- `size` is the logical uncompressed bytes covered by this frame.
- `wsize` is the exact payload byte count that follows the header newline.
- For `comp=none`, `size` must equal `wsize`.
- For `enc=age`, decrypt before applying `comp`.
- For `comp=zstd|lz4`, decompressed bytes must equal `size`.
- Trailer `hash=<algo>:<value>` is used for frame-integrity validation/logging.
- `deadline:<duration>` limits how long receiver should allow this frame to complete; exceeded deadline is a protocol timeout.

## Error Handling

Receiver must close the TCP connection on framing errors:

- malformed version token
- malformed property syntax
- invalid numeric conversion
- invalid `deadline` duration
- payload shorter/longer than declared `wsize`
- decompressed segment length not equal to declared `size`

Receiver should emit protocol error code in logs with offending `file_id` when available.

## Versioning

- `FX/1` is the current version.
- Breaking framing changes require new token (`FX/2`).
- New optional properties are backward-compatible within `FX/1`.

## Response Framing

Receiver responses use a three-part frame:

1. Response header line
2. Exactly `wsize` payload bytes
3. Response trailer line

Both header and trailer are single lines terminated by `\n`.

Format:

```text
FXR/1 <file_id> <properties...>
```

- `FXR/1`: response protocol version token.
- `<file_id>`: same manifest-relative file ID from request.
- `<properties...>`: space-separated properties.

Example:

```text
FXR/1 12 status=ok comp=zstd enc=age offset=0 size=1048576 wsize=262144 elapsed=420ms
<262144 payload bytes>
FXT/1 12 status=ok hash=xxh64:1af3bc9d00ee42aa
```

### Required Response Properties

- `status=<code>`: `ok` or error code (for example `timeout`, `bad_frame`, `checksum_mismatch`).
- `offset=<n>`: segment offset processed.
- `size=<n>`: actual logical bytes processed.
- `wsize=<n>`: exact number of payload bytes that follow header.

### Optional Response Properties

- `comp=<mode>`: compression mode actually applied.
- `enc=<mode>`: encryption mode actually applied.
- `deadline:<duration>`: effective deadline used for this segment.
- `elapsed=<duration>`: observed processing duration.
- `detail=<token>`: machine-readable short detail code.

### Response Trailer

Trailer format:

```text
FXT/1 <file_id> <properties...>
```

Trailer is used for realized post-transfer metadata, especially checksums.

Common trailer properties:

- `status=<code>`: final trailer status (`ok` or error code).
- `hash=<algo>:<value>`: frame checksum value.
- `detail=<token>`: optional machine-readable detail.

### Response Semantics

- `size` and `wsize` in response are authoritative observed values.
- Receiver must emit exactly `wsize` payload bytes after `FXR/1`.
- Trailer is emitted after payload and carries final checksum values.
- `status=ok` indicates segment accepted and written.
- Non-`ok` status indicates segment rejected or incomplete; sender should treat as failed for retry logic.
- Trailer `hash=<algo>:<value>` is the frame checksum token for this frame payload and framing bytes.

## `/fs/file` Contract

`GET /fs/file/{txferid}/{fid}` returns one or more frame triplets:

1. `FX/1` header line
2. `wsize` payload bytes (raw or compressed per `comp`)
3. `FXT/1` trailer line

The server repeats these triplets until the requested window is complete.
Default logical frame size cap is `8 MiB`, so large responses are split into
multiple frames.

For `/fs/file` responses, header properties are emitted in this order:
`offset`, `size`, `wsize`, `comp`, `enc`, `hash`, optional `max-wsize`, then `ts`.

Current trailer shape:

```text
FXT/1 <file_id> status=ok ts=<unix_ms> hash=<algo>:<value> [file-hash=<algo>:<value>] next=<offset>
```

`next` is the offset that the following frame starts at (`offset + size`).
The final trailer uses `next=0` as a terminal marker.

`file-hash` is emitted on final trailer as the full-file checksum token for the served file window.
The final trailer also includes file metadata tokens:
`meta:size`, `meta:mtime_ns`, `meta:mode`, `meta:uid`, `meta:gid`, `meta:user`, `meta:group`.
Clients may use `meta:mode`, `meta:uid`, and `meta:gid` to mirror ownership/permissions
only after payload integrity verification succeeds.

`hash=<algo>:<value>` is the canonical checksum token for `/fs/file` trailers.
For `/fs/file`, trailer hash is `hash=xxh64:<hex16>` and covers:
- header line bytes (including trailing `\n`)
- payload bytes (`wsize`)
- trailer prefix bytes up to but not including ` hash=...`

`file-hash=<algo>:<value>` on terminal trailer is the full logical object checksum.
Current implementation emits `file-hash=xxh128:<hex32>`.

`max-wsize` is a first-frame hint only. Clients may use it to pre-size a reusable
frame buffer, but they may cap allocation (current client default cap is `64 MiB`)
and still stream larger frames in multiple reads.

## `/fs/file` Ack Semantics

`ack-bytes` is interpreted as:

- `-1` for missing-file acknowledgement.
- `<bytes>@<server_ts_ms>` for positive progress acknowledgement.
- `<bytes>@<server_ts_ms>@<algo>:<value>` for final completion acknowledgement with full-file hash.

Legacy positive numeric-only acks are not accepted.

Ack-only calls are supported using:

```text
GET /fs/file/{txferid}/{fid}?path=<abs>&offset=0&size=0&ack-bytes=...
```

Successful ack-only requests return `204 No Content` and no frame payload.

## `/fs/file/{txferid}/{fid}/checksum` Contract

`GET /fs/file/{txferid}/{fid}/checksum` returns framing lines only (no payload bytes):

1. `FX/1` header line
2. zero payload bytes (`wsize=0`)
3. `FXT/1` trailer line

The response emits one frame per checksum window and flushes after every frame when possible.

Query parameters:

- `path=<abs>` (required)
- `window-size=<bytes>` (optional, default `min(64 MiB, file_size)`)
- `checksum=<algo>` (optional, repeatable and comma-separated)
  - supported: `none`, `xxh128`, `xxh64`
  - default: `xxh128`

Checksum trailer semantics:

- Per-window frames include repeated `file-hash=<algo>:<value>` tokens as rolling cumulative checksum snapshots.
- Terminal frame (`next=0`) includes final `file-hash=<algo>:<value>` values.
- Every frame still includes `hash=xxh64:<hex16>` as frame integrity checksum.
- Terminal frame includes the same metadata tokens as `/fs/file`.
