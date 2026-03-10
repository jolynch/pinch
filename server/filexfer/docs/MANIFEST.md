# Filexfer Manifest Specification (Draft v1)

This document defines the manifest format used to compactly
describe files to transfer from a server.

When requesting a manifest via `TXFER`, clients may first issue `AUTH`.
If `AUTH` provides a client recipient, the manifest response stream is age-encrypted
for that recipient and must be decrypted by the client before parsing this format.

## Structure

Manifest is line-oriented UTF-8 text:

1. First line: manifest header with encoding version
2. Following lines: one file entry per line

Empty lines are ignored. Lines beginning with `#` are comments and ignored.

## Header

Format:

```text
FM/1 <transfer_id> <root>
```

- `FM/1` is the manifest encoding/version token.
- `<transfer_id>` is the transfer identifier associated with this manifest.
- `<root>` is a length-prefixed transfer root directory name in `<n>:<data>` format.
- Any other token is invalid for this version.

## Entry Lines

Format:

```text
<id> <size> <mtime> <mode> <path>
```

- `<id>`: unsigned integer file ID.
- `<size>`: file size in bytes (unsigned integer).
- `<mtime>`: front-coded mtime token using `<prefix_len>:<suffix_data>` format.
- `<mode>`: octal Unix permission/mode bits (`0000`-`7777`), including setuid/setgid/sticky when present.
- `<path>`: front-coded path token using `<prefix_len>:<suffix_len>:<suffix_data>` format.

Fields are separated by a single ASCII space.

### Root Directory

Header root encoding format:

```text
<n>:<root-data>
```

- `<n>` is decimal byte length of `<root-data>`.
- `<root-data>` may contain spaces and arbitrary UTF-8 characters (except newline).
- `<root-data>` identifies the transfer root directory name.
- All entry resolved paths are interpreted relative to this root.

### Mtime Encoding (Front Coding)

Mtime token format:

```text
<prefix_len>:<suffix_data>
```

- `<prefix_len>` is decimal number of bytes reused from previous resolved mtime string.
- `<suffix_data>` is decimal digits.
- Resolved mtime string is:
  - first entry: `<suffix_data>` (and `<prefix_len>` must be `0`)
  - later entries: `prev_mtime[:prefix_len] + suffix_data`

Resolved mtime must parse as unsigned Unix nanoseconds.

Example:

- Previous mtime: `1735771234567890123`
- Token: `12:8909999`
- Decoded: `1735771234568909999`

## Path Rules (Front Coding)

Path token format:

```text
<prefix_len>:<suffix_len>:<suffix_data>
```

- `<prefix_len>` is decimal number of bytes reused from the previous resolved path.
- `<suffix_len>` is decimal byte length of `<suffix_data>`.
- `<suffix_data>` may contain spaces and arbitrary UTF-8 characters (except newline).
- Resolved path is:
  - first entry: `<suffix_data>` (and `<prefix_len>` must be `0`)
  - later entries: `prev_path[:prefix_len] + suffix_data`

Example:

```text
0 100 1735771000 0644 0:9:dir/a.txt
1 200 1735771001 0600 4:5:b.txt
2 300 1735771002 0755 5:9:sub/c.txt
```

Resolves to:

- `0` -> `dir/a.txt`
- `1` -> `dir/b.txt`
- `2` -> `dir/sub/c.txt`

Resolved path safety rules:

- Resolved paths are relative to the manifest root (no leading `/`).
- `..` path traversal is not allowed.
- `\` is not allowed; use `/`.

## Validation Rules

- Header must have exactly 3 fields: `FM/1`, `<transfer_id>`, and `<root>`.
- Header root must parse as `<n>:<data>` and byte length must match.
- IDs must be unique.
- IDs should be monotonically increasing.
- `size` must parse as unsigned integer.
- `mtime` field must parse as `<prefix_len>:<suffix_data>`.
- `mode` must parse as octal and be <= `07777`.
- For first entry mtime, `prefix_len` must be `0`.
- For later entries mtime, `prefix_len` must be <= byte length of previous resolved mtime string.
- Mtime `suffix_data` must be decimal digits.
- Decoded mtime must parse as unsigned integer Unix nanoseconds.
- Each entry must have exactly 5 fields.
- Path field must parse as `<prefix_len>:<suffix_len>:<suffix_data>`.
- For first entry, `prefix_len` must be `0`.
- For later entries, `prefix_len` must be <= byte length of previous resolved path.
- `len(<suffix_data> bytes)` must equal `suffix_len`.
- After path resolution, resulting path must remain relative and traversal-safe.

## Example Manifest

```text
FM/1 9f83ab12 8:repo-root
0 4096 0:1735771234567890123 0644 0:14:data/chunk-000
1 4096 14:90123 0644 11:3:001
2 1024 15:1350 0644 11:3:002
3 88 0:1736000000000000000 0600 0:15:logs/result.txt
```
