---
title: 'CDC Chunking'
---

Content-Defined Chunking: chunk boundaries are determined by file content, not fixed byte offsets

## What CDC Is

CDC stands for Content-Defined Chunking.
Unlike fixed-size chunking, CDC scans the file content and identifies chunk boundaries based on data patterns (rolling hash fingerprints)

Because boundaries are content-driven, when a large file is only changed locally — such as inserting or deleting a small region in the middle,
or appending data at the end — many unchanged regions can still be cut into the same chunks as before.
Those identical chunks are reused from existing storage, which improves deduplication rate significantly

CDC has no assumption about the internal structure of the file.
It works for any kind of local modification: insertion, deletion, or in-place update in any position

## FastCDC

FastCDC is a specific algorithm implementing CDC, and the one adopted by Prime Backup.
It was first described in a [paper at USENIX ATC 2016](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf),
with further improvements published in a [2020 follow-up](https://ieeexplore.ieee.org/document/9055082)

At its core, FastCDC uses a gear hash — a lightweight rolling hash that processes data one byte at a time
via a simple table lookup and bit shift — to detect chunk boundaries based on a bitmask condition on the hash value

What sets FastCDC apart from earlier CDC algorithms is its normalized chunking technique.
Rather than applying a single hash mask throughout, it uses a stricter mask below the average size target and a more permissive one above it,
nudging the chunk size distribution toward the desired average without sacrificing the content-adaptive nature of CDC

Prime Backup uses [`pyfastcdc`](https://github.com/Fallen-Breath/pyfastcdc), a Cython-accelerated Python implementation of FastCDC 2020
that delivers near-native chunking throughput

## Available Algorithms

| Algorithm      | Avg Chunk Size | Min Chunk Size | Max Chunk Size |
|----------------|----------------|----------------|----------------|
| `fastcdc_32k`  | 32 KiB         | 8 KiB          | 256 KiB        |
| `fastcdc_128k` | 128 KiB        | 64 KiB         | 1 MiB          |

`fastcdc_32k` is the default and works well for most use cases
`fastcdc_128k` uses a coarser granularity and is better suited for very large files (10 GiB or more) where the per-chunk metadata overhead of `fastcdc_32k` becomes noticeable

Both algorithms use FastCDC with normalized chunking and a fixed seed (`0`) for reproducibility

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
