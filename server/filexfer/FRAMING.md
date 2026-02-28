# Filexfer Framing Specification (Draft v1)

This document defines the wire framing for fast efficient
file transfers over plain TCP sockets.

## Scope

- Transport: plain TCP stream.
- This framing is for per-file transfer messages after a manifest has been exchanged out of band.
- `file_id` is an integer index into that exchanged manifest (0-based).

## Frame Structure

Each file transfer frame is:

1. Header line (ASCII, UTF-8 safe)
2. Raw payload bytes

Header line is terminated by `\n`.

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
FX/1 12 comp=zstd enc=age offset=0 size=1048576 wsize=262144 xsum:xxh3 deadline:30s
```

## Properties

Properties are ASCII and case-sensitive.

### Required

- `comp=<mode>`: compression mode.
- `enc=<mode>`: encryption mode.
- `offset=<n>`: byte offset within the logical file where payload data belongs.
- `size=<n>`: number of original (logical, uncompressed) bytes represented by this frame.
- `wsize=<n>`: number of payload bytes on the wire for this frame.

### Optional

- `xsum:<algo>`: checksum algorithm declaration (caller may not know checksum value up front).
  - Supported algorithms: `xxh3`, `blake3`, `xxh128`, `sha256`.
  - At most one `xsum` token per frame.
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

- `mtime` and full-file `size` are manifest properties, not framing properties.
- `offset` allows resumable/partial writes.
- `size` is the logical uncompressed bytes covered by this frame.
- `wsize` is the exact payload byte count that follows the header newline.
- For `comp=none`, `size` must equal `wsize`.
- For `enc=age`, decrypt before applying `comp`.
- For `comp=zstd|lz4`, decompressed bytes must equal `size`.
- `xsum:<algo>` declares the checksum algorithm to use for validation/logging when checksum data is computed later.
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
FXT/1 12 status=ok xsum:xxh3=9f12ab... xsum:blake3=4ac9...
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
- `xsum:<algo>=<value>`: computed checksum values.
- `detail=<token>`: optional machine-readable detail.

### Response Semantics

- `size` and `wsize` in response are authoritative observed values.
- Receiver must emit exactly `wsize` payload bytes after `FXR/1`.
- Trailer is emitted after payload and carries final checksum values.
- `status=ok` indicates segment accepted and written.
- Non-`ok` status indicates segment rejected or incomplete; sender should treat as failed for retry logic.
- Trailer `xsum:<algo>=<value>` entries correspond to requested checksum algorithms when declared in request.
