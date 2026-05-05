---
title: 'File Chunking'
---

Split large files into smaller chunks for better deduplication across backups

## What File Chunking Is

File chunking is a storage strategy where a large file is split into smaller pieces called chunks before being stored.
Each chunk is hashed and deduplicated independently, so when only part of a large file changes between backups,
only the modified chunks need to be written anew. The unchanged chunks are reused directly from existing storage.

In Prime Backup, restoring a chunked file is transparent to users.
The original file is reconstructed automatically when the backup is read or exported.

## When It Is Applied

Chunking is controlled by two config fields inside `backup`:

- `chunking_enabled`: the master switch for chunking. If `false`, no file will ever be chunked
- `chunking_rules`: an ordered list of rules. For each file, Prime Backup walks through this list and applies the first matching rule

A rule matches when both conditions are true:

- the file size is at least `file_size_threshold`
- the file path relative to `source_root` matches the rule's `patterns`

If no rule matches, the file is stored as a regular direct blob without chunking.

The default configuration is:

```json
{
    "chunking_enabled": false,
    "chunking_rules": [
        {
            "algorithm": "fastcdc_32k",
            "file_size_threshold": 104857600,
            "patterns": [
                "**/*.db"
            ]
        }
    ]
}
```

Changing these options only affects files newly stored in future backups.
Existing direct blobs or chunked blobs will not be converted automatically.

## How It Is Stored

Prime Backup still creates one blob record for the whole file, but the blob uses the `chunked` storage method instead of `direct`.

The current implementation works in the following order:

1. Cut the file into chunks using the selected algorithm
2. Calculate a BLAKE3 hash for each chunk
3. Reuse chunks that already exist in storage
4. Compress and write only the new chunks
5. Bind the ordered chunk list back to the whole-file blob (by offset)

### Metadata Optimization (Chunk Groups)

Conceptually, a chunked blob is just an ordered list of chunks.
Storing a direct binding row for every blob-chunk pair would be expensive,
so the implementation groups consecutive chunks into chunk groups and stores two bindings:

- blob -> chunk group (by blob offset)
- chunk group -> chunk (by group offset)

```
+--------------------------------------------------------------------------------+
|                                      blob                                      |
+--------------------------+-----------------------------------------------------+
|       chunk group 1      |  chunk group 2  |          chunk group 3            |
+--------------------------+-----------------+-----------------+-----------------+
| chunk1 | chunk2 | chunk3 | chunk4 | chunk5 | chunk6 | chunk7 | chunk8 | chunk9 |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+
```

This reduces metadata overhead without changing the logical model.

Chunk hashes and chunk group hashes always use `blake3`, while the whole-file blob hash still follows `backup.hash_method`.

## Compression and Performance

Chunking does not disable compression.

For a chunked blob:

- the blob record itself uses `plain` as its own compression marker
- each chunk is compressed independently according to `backup.compress_method` and `backup.compress_threshold`
- the blob `stored_size` is the sum of unique stored chunk sizes

Compared with direct blob storage, chunked storage is slower on the first backup of a file,
because Prime Backup needs extra work to cut the file, calculate hashes, and process each chunk.
The benefit becomes apparent on subsequent backups where many chunks can be reused.

## Available Algorithms

| Algorithm      | Type  | Avg Chunk Size | Good For                                                                            |
|----------------|-------|----------------|-------------------------------------------------------------------------------------|
| `fastcdc_32k`  | CDC   | 32 KiB         | general-purpose; any locally modified large file                                    |
| `fastcdc_128k` | CDC   | 128 KiB        | very large files (10 GiB or more) where 32 KiB granularity produces too many chunks |
| `fixed_4k`     | Fixed | 4 KiB          | MC region files (matches 4 KiB page boundaries); note: causes severe metadata bloat |
| `fixed_32k`    | Fixed | 32 KiB         | medium fixed-size use cases                                                         |
| `fixed_128k`   | Fixed | 128 KiB        | append-write files with predictable end-growth                                      |

See the detailed pages for each approach:

- [CDC Chunking](chunking_cdc.md): content-aware chunk boundaries; works well for any kind of local modification
- [Fixed-Size Chunking](chunking_fixed.md): fixed byte-offset boundaries; simpler but less adaptive (alpha)

## Observation

Prime Backup maintenance logic already understands chunked storage.
You can inspect the effect with `!!pb database overview`, which includes a dedicated chunk statistics section.

If Prime Backup finds that one chunked file produced many brand new chunks in a single backup, it will emit a warning in logs.
That usually means the file is not a good chunking target, unless this is the first backup containing that file.
