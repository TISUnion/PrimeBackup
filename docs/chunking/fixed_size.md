---
title: 'Fixed-Size Chunking'
---

!!! note "Beta"

    Fixed-size chunking is a beta feature.
    The overall functionality is usable, but algorithm implemetation and behavior may change in future releases.
    Don't worry, data created with fixed-size chunking will remain fully compatible and accessible in future versions.
    Note that `fixed_auto` is in alpha status; see its section below for details

Fixed-size chunking splits files at predictable byte-offset boundaries, with every chunk being exactly the configured size
(the last chunk may be smaller if the file size is not a multiple of the chunk size)

## What Fixed-Size Chunking Is

Fixed-size chunking is conceptually simple: the file is divided into equal-sized pieces from start to end.
Each piece is hashed and stored independently, just like CDC chunks

Unlike CDC, chunk boundaries do not shift when content is inserted or deleted in the middle of the file.
Any edit before the end of a chunk changes that chunk's hash entirely, and any insertion or deletion causes all subsequent chunks to shift,
potentially invalidating a large number of previously stored chunks

This means fixed-size chunking is generally inferior to CDC for files with arbitrary edits.
Its benefit is only realized in scenarios where the file's write pattern is well-aligned to chunk boundaries

For example, with `fixed_4k` applied to a Minecraft region file:

```
+----------------------------------------------------------------------+
|                    file (e.g. r.0.0.mca)                             |
+------+------+------+------+------+------+------+------+------+-- - --+
| 4KiB | 4KiB | 4KiB | 4KiB | 4KiB | 4KiB | 4KiB | 4KiB | 4KiB |  ...  |
|  c1  |  c2  |  c3  |  c4  |  c5  |  c6  |  c7  |  c8  |  c9  |       |
+------+------+------+------+------+------+------+------+------+-- - --+
```

Each 4 KiB chunk corresponds to one internal page of the region file.
When only a few game chunks change between backups, only the corresponding pages are dirtied,
and the rest of the chunks are identical to those already stored

## Available Algorithms

| Algorithm    | Chunk Size      | Typical Use Case                                                                                                                            |
|--------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `fixed_4k`   | 4 KiB           | Minecraft region files (`.mca`): each region file is organized in 4 KiB pages, so changes in one chunk only invalidate that 4 KiB page      |
| `fixed_32k`  | 32 KiB          | General intermediate granularity                                                                                                            |
| `fixed_128k` | 128 KiB         | Append-only files: growth at the tail only creates new trailing chunks, leaving all previous chunks intact                                  |
| `fixed_1m`   | 1 MiB           | Very large append-only files: even lower metadata overhead than `fixed_128k`, useful when fine-grained deduplication is not required        |
| `fixed_auto` | 128 KiB / 4 KiB | Adaptive fixed-size strategy that uses the previous backup's same-path chunk layout to limit metadata growth while keeping some 4 KiB reuse |

### fixed_4k

The 4KiB chunk size aligns with the internal page structure of Minecraft's Anvil region files (`.mca`).
In theory, modifying a small number of chunks in the game only dirties a limited number of 4 KiB pages,
making `fixed_4k` capable of the finest-grained deduplication for region files

However, `fixed_4k` has serious practical drawbacks:

- extremely high metadata overhead: a 1 GiB file requires roughly 262 144 chunk records
- poor I/O performance: each chunk requires a separate read-write cycle during backup

Unless the file is very large and only a tiny number of pages change per backup, `fixed_4k` is unlikely to be worth the cost

### fixed_32k

A middle-ground option. Metadata overhead is 32× lower than `fixed_4k` but granularity is also much coarser

### fixed_128k

The 128 KiB chunk size is well-suited for files that grow by appending data at the end.
When new data is appended, only the trailing chunks change; all preceding chunks retain the same hash and are reused

This makes `fixed_128k` a reasonable alternative to CDC for pure append-write files

### fixed_1m

The 1 MiB chunk size further reduces metadata overhead compared to `fixed_128k`, at the cost of coarser deduplication granularity.
It is suitable for extremely large append-only files where even the 128 KiB metadata overhead becomes a concern

For most use cases, `fixed_128k` or CDC variants are preferred. Consider `fixed_1m` only when the file is very large and write patterns are exclusively append-only

### fixed_auto

!!! warning "Alpha"

    `fixed_auto` is in alpha status and is not well optimized for performance

`fixed_auto` walks the file in 128 KiB windows. For each full window, it checks the previous backup's same-path chunk layout at the same offset:

- if the previous window was one 128 KiB chunk and the current content is unchanged, it keeps one 128 KiB chunk
- if the previous window was one 128 KiB chunk and the current content changed, it stores the current window as thirty-two 4 KiB chunks
- if the previous window was thirty-two 4 KiB chunks, it compares the 4 KiB hashes first; when none changed, it stores one 128 KiB chunk, otherwise it keeps thirty-two 4 KiB chunks

Missing previous data, direct blobs, irregular previous layouts, and incomplete tail windows are stored as one chunk for that window

With this, `fixed_auto` can achieve the following effect: for parts of a file that keep changing, it performs chunk-level deduplication at 4 KiB granularity;
for other parts, it performs chunk-level deduplication at 128 KiB granularity

Since region files (`.mca`) in Minecraft saves are modified at 4 KiB granularity,
`fixed_auto` is expected to achieve deduplication close to `fixed_4k` without introducing excessive metadata overhead

## Poor Candidates

Fixed-size chunking is a poor choice for:

- files that are frequently modified in the middle or beginning (insertion/deletion shifts all subsequent chunks)
- files with completely unpredictable byte-level change patterns
- files where the chunk size does not align with any meaningful internal structure

## No Extra Dependencies

Fixed-size chunking has no additional Python dependency requirements.
It is available as long as Prime Backup is installed
