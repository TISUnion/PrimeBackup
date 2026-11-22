---
title: 'CDC Chunking'
---

Content-Defined Chunking: chunk boundaries are determined by file content, not fixed byte offsets

!!! note "Beta"

    CDC chunking is a beta feature.
    The overall functionality is usable, but algorithm implementation and behavior may change in future releases.
    Don't worry, data created with CDC chunking will remain fully compatible and accessible in future versions

## What CDC Is

CDC stands for Content-Defined Chunking.
Unlike fixed-size chunking, CDC scans the file content and identifies chunk boundaries based on rolling hash fingerprints

Because boundaries are content-driven, local edits such as inserting or deleting bytes in the middle, or appending data at the end, leave most unchanged regions aligned to the same chunks as before.
Those identical chunks are reused from existing storage, significantly improving deduplication

CDC makes no assumptions about a file's internal structure.
It works for any kind of local modification: insertion, deletion, or in-place updates at any position

## FastCDC

FastCDC is the CDC algorithm adopted by Prime Backup.
It was introduced in a [USENIX ATC 2016 paper](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf) and refined in a [2020 follow-up](https://ieeexplore.ieee.org/document/9055082)

FastCDC uses a lightweight rolling hash (gear hash) that scans one byte at a time via a table lookup and bit shift, checking a bitmask condition to locate chunk boundaries.
It also uses normalized chunking: a stricter mask below the target average size and a looser mask above it, keeping chunk sizes closer to the target average while preserving content-defined boundaries

Prime Backup uses [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc), a Cython-accelerated Python implementation of FastCDC 2020 with near-native throughput

## Available Algorithms

| Algorithm      | Avg Chunk Size | Min Chunk Size | Max Chunk Size |
|----------------|----------------|----------------|----------------|
| `fastcdc_32k`  | 32 KiB         | 8 KiB (/4)     | 256 KiB (*8)   |
| `fastcdc_128k` | 128 KiB        | 32 KiB (/4)    | 1 MiB (*8)     |
| `fastcdc_1m`   | 1 MiB          | 256 KiB (/4)   | 4 MiB (*4)     |

- `fastcdc_32k` is the finest-grained CDC preset and works well for most CDC use cases
- `fastcdc_128k` uses a coarser granularity and is better suited for very large files (10 GiB or more) where the per-chunk metadata overhead of `fastcdc_32k` becomes noticeable
- `fastcdc_1m` uses a 1 MiB average chunk size to further cut metadata overhead for extremely large files, with coarser deduplication granularity

All three algorithms use FastCDC with normalized chunking and a fixed seed (`0`) for reproducibility

## Good Candidates

CDC works well whenever most backups only change part of a file, for example:

- large database files with local row-level updates
- large log files that are appended at the end and need to be backed up
- any large file that is frequently modified in a local, non-global manner

## Poor Candidates

CDC is usually not a good fit when:

- the file is completely rewritten on every save (no local structure is preserved)
- the file is a compressed or encrypted container, where any small content change scrambles a large byte region

Also note that the first backup containing a file still needs to write all chunks,
so CDC benefits only become visible on later backups with high chunk reuse

## Dependencies

CDC chunking requires the optional Python library [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc).
You can install it directly, or install the optional dependency bundle:

```bash
pip3 install pyfastcdc
# or install all optional dependencies at once
pip3 install -r requirements.optional.txt
```
