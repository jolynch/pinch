# Filexfer Manifest Specification (FM/2)

This document defines the strict manifest format emitted by `TXFER` and consumed by `start`/`get`.

When requesting a manifest via `TXFER`, clients may first issue `AUTH`.
If `AUTH` provides a client recipient, the manifest stream is age-encrypted for that recipient.

## Structure

Manifest is line-oriented UTF-8 text:

1. Header line (`FM/2`)
2. Entry lines (one file per line)

Empty lines and `#` comments are ignored.

## Header

Format:

```text
FM/2 <transfer_id> <root-len:root> mode=<fast|gentle> link-mbps=<int> concurrency=<int>
```

Header fields are required.

- `FM/2`: manifest version token.
- `<transfer_id>`: transfer identifier.
- `<root-len:root>`: length-prefixed root path token (`<n>:<data>`).
- `mode`: transfer mode (`fast` or `gentle`).
- `link-mbps`: client-reported link estimate in Mbps (`>= 0`).
- `concurrency`: planned client concurrency (`> 0`).

Any unknown header option is invalid.

## Entry Lines

Format:

```text
<id> <size> <mtime> <mode> <path>
```

- `<id>`: unsigned file id.
- `<size>`: file size bytes (unsigned integer).
- `<mtime>`: front-coded mtime token: `<prefix_len>:<suffix_data>`.
- `<mode>`: octal unix mode bits (`0000`-`7777`).
- `<path>`: front-coded path token: `<prefix_len>:<suffix_len>:<suffix_data>`.

Fields are separated by one ASCII space.

## Root Token

```text
<n>:<root-data>
```

- `<n>` is decimal byte length of `<root-data>`.
- `<root-data>` may contain spaces and UTF-8 bytes except newline.

## Mtime Front Coding

Token format:

```text
<prefix_len>:<suffix_data>
```

Decoded value:

- first entry: `<suffix_data>` (`prefix_len` must be `0`)
- next entries: `prev_mtime[:prefix_len] + suffix_data`

Decoded mtime must be unsigned Unix nanoseconds.

## Path Front Coding

Token format:

```text
<prefix_len>:<suffix_len>:<suffix_data>
```

Decoded value:

- first entry: `<suffix_data>` (`prefix_len` must be `0`)
- next entries: `prev_path[:prefix_len] + suffix_data`

Path constraints:

- must be relative (no leading `/`)
- `..` traversal is rejected
- `\\` is rejected; use `/`

## Validation Rules

- Header must be `FM/2` and include required metadata fields.
- Header root token must parse and length-match.
- Unknown header options are rejected.
- Entry IDs must be unique and strictly increasing.
- `size` must be unsigned and fit `int64`.
- `mtime` token must decode to decimal digits and fit `int64`.
- `mode` must be octal and `<= 07777`.
- Each entry must have exactly 5 fields.
- Path token must parse and remain traversal-safe after decode.

## Example

```text
FM/2 9f83ab12 8:repo-root mode=fast link-mbps=1200 concurrency=16
0 4096 0:1735771234567890123 0644 0:14:data/chunk-000
1 4096 14:90123 0644 11:3:001
2 1024 15:1350 0644 11:3:002
3 88 0:1736000000000000000 0600 0:15:logs/result.txt
```
