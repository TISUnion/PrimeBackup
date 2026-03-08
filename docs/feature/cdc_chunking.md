---
title: 'CDC Chunking'
---

Use CDC for large files that are frequently modified locally

## What CDC Is

CDC stands for `Content-Defined Chunking`.
It is a chunking technique that decides chunk boundaries from the file content itself instead of fixed byte offsets.

Because of that, when a large file is only changed locally, such as appending at the end or editing a small region in the middle,
many unchanged parts can still be cut into the same chunks as before.
Those chunks keep the same hash and can be reused across backups, which improves deduplication

In Prime Backup, restoring a chunked file is transparent to users.
The original file is reconstructed automatically when the backup is read or exported

## When It Is Applied

CDC chunking is used only when all of the following conditions are true:

- `backup.cdc_enabled` is `true`
- the file size is greater than `0`
- the file size is at least `backup.cdc_file_size_threshold`
- the file path relative to `backup.source_root` matches `backup.cdc_patterns`

The default configuration is:

```json
{
    "cdc_enabled": false,
    "cdc_file_size_threshold": 104857600,
    "cdc_patterns": [
        "**/*.db"
    ]
}
```

## How It Is Stored

Prime Backup still creates one blob record for the whole file, but the blob uses the `chunked` storage method instead of `direct`.

The current implementation works in the following order:

1. Cut the file with FastCDC
2. Calculate a BLAKE3 hash for each chunk
3. Reuse chunks that already exist in storage
4. Compress and write only the new chunks
5. Bind the ordered chunk list back to the whole-file blob (by offset)

The current chunking parameters are fixed in code:

- average chunk size: `256 KiB`
- minimum chunk size: `64 KiB`
- maximum chunk size: `1 MiB`

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

This reduces metadata overhead without changing the logical model

Chunk hashes and chunk group hashes always use `blake3`, while the whole-file blob hash still follows `backup.hash_method`

## Compression and Performance

CDC chunking does not disable compression

For a chunked blob:

- the blob record itself uses `plain` as its own compression marker
- each chunk is compressed independently according to `backup.compress_method` and `backup.compress_threshold`
- the blob `stored_size` is the sum of unique stored chunk sizes

Compared with direct blob storage, CDC is slower in the current implementation.
Before new data is written, Prime Backup needs extra work to cut the file, calculate hashes, and read chunk contents again for chunk creation and verification

Because of that, CDC is best reserved for files that are large, frequently modified locally, and worth backing up

## Good Candidates

CDC is a good fit when most backups only change part of a large file, for example:

- large database files with local updates
- large log files that are appended at the end and need to be backed up
- files that are often modified by local insertion, deletion, or small-range updates

## Poor Candidates

CDC is usually not a good fit when the whole file changes almost everywhere in each backup, for example:

- files that are rewritten completely every time
- exported artifacts whose entire content layout changes on each generation
- already compressed or encrypted files with poor chunk reuse after edits

Also note that the first backup containing a file still needs to write all chunks,
so CDC mainly pays off on later backups with high chunk reuse

If Prime Backup finds that one chunked file produced many brand new chunks, it will emit a warning in logs.
That usually means the file is not a good CDC target, unless this is the first backup containing that file

## Dependencies and Observation

CDC chunking requires the optional Python dependency `pyfastcdc`.
You can install it directly, or install the optional dependency bundle from `requirements.optional.txt`.

Prime Backup maintenance logic already understands chunked storage.
You can inspect the effect with `!!pb database overview`, which includes a dedicated chunk statistics section