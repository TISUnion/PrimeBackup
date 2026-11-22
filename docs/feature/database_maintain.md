---
title: 'Database Maintenance'
---

Consistency validation and database maintenance

## Database Validation

| Command                           | Description                                                                             |
|-----------------------------------|-----------------------------------------------------------------------------------------|
| `!!pb database validate packs`    | Validate pack file existence, size, and live statistics                                 |
| `!!pb database validate blobs`    | Validate blob integrity, including stored size and content hash                         |
| `!!pb database validate chunks`   | Validate chunks, chunk groups, and their relation bindings                              |
| `!!pb database validate files`    | Validate file objects and their references to blobs and filesets                        |
| `!!pb database validate filesets` | Validate fileset objects and their references to backups and files                      |
| `!!pb database validate backups`  | Validate backup objects and their references to filesets                                |
| `!!pb database validate all`      | Run all validations above                                                               |

Example:

```
!!pb database validate all
```

Example output:

```
> !!pb database validate all
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO]: [PB] Start validating packs, please wait...
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Pack validation start
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 16 / 16 packs
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Pack validation done: total 16, validated 16, ok 16, bad 0
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO]: [PB] All 16 packs are healthy
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO]: [PB] Start validating blobs, please wait...
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:16:20] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 4325 blobs are healthy
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Start validating chunks (chunks, chunk groups, relation bindings), please wait...
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Chunk validation start
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 0, validated 0, ok 0, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 0 chunks, 0 chunk groups, 0 + 0 bindings are healthy
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Start validating files (including file, directory and symlink), please wait...
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: File validation start
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 9147 / 9147 file objects
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: File validation done: total 9147, validated 9147, ok 9147, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validated 9147 / 9147 files
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 9147 files are healthy
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Start validating filesets, please wait...
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Fileset validation start
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 26 fileset objects
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 26 / 26 fileset objects
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Fileset validation done: total 26, validated 26, ok 26, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validated 26 / 26 filesets
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 26 filesets are healthy
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Start validating backups, please wait...
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Backup validation start
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 21 backup objects
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Backup validation done: total 21, validated 21, ok 21, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validated 21 / 21 backups
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 21 backups are healthy
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validation done, cost 3.39s. packs: good, blobs: good, chunks: good, files: good, filesets: good, backups: good
```

### What Gets Validated

Packs:

- File existence: verify that pack files exist in storage
- Size match: verify that on-disk pack sizes match database records
- Live stats: verify that live size and live entry count do not exceed totals

Direct Blobs:

- File existence: verify that direct blob files exist in storage
- Integrity: verify content hashes of stored blobs
- Size match: verify that stored sizes match database records
- Compression check: verify that compressed data can be decompressed intact

For chunked blobs, `validate blobs` checks layout and size statistics.
To check the integrity of chunk contents as well, run `validate chunks`

Chunks:

- Pack reference: verify that each chunk references an existing pack entry
- Integrity: verify chunk data read from pack entries
- Size match: verify that stored sizes match database records
- Orphan detection: check for chunks not referenced by any chunk group and chunk groups not referenced by any blob
- Binding consistency: verify chunk-group-to-chunk and blob-to-chunk-group bindings

Files:

- Reference integrity: verify that blobs referenced by files exist
- Fileset association: verify references between files and filesets
- Metadata consistency: verify that the blob information stored in the file matches the blob record

Filesets:

- Backup association: verify references between filesets and backups
- File count: verify the number of files recorded in each fileset
- Size calculation: verify fileset size statistics

Backups:

- Fileset reference: verify that filesets referenced by backups exist
- File count: verify backup file counts against the filesets
- Size match: verify backup sizes against fileset statistics


When validation detects issues, detailed error information will be displayed:

```
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4322, bad 3
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] Found 3 / 4325 bad blobs in total
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] Missing blob amount: 2
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] 1. 4fc02ad5e7508949634f012fb96c2968: blob file does not exist
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] 2. 54695a153daae89e685894ee966a277e: blob file does not exist
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] Mismatched blob amount: 1
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] 1. 8b36d71e250e25527f59b8fd9e0f2dce: stored size mismatch, expect 178, found 176
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] Affected range: 6 / 9147 file objects, 2 / 26 filesets, 16 / 21 backups
[MCDR] [23:18:49] [PB@fc91-worker-heavy/INFO]: [PB] See log file pb_files/logs/validate.log for details and affected stuffs of these bad blobs
```


## Database Prune & Compaction

### Database Prune

Remove unused records such as orphan objects from the database:

```
!!pb database prune
```

Items handled during pruning:

- Orphan blobs: blobs no longer referenced by any file
- Orphan files: files no longer referenced by any fileset
- Orphan filesets: filesets no longer referenced by any backup
- Base fileset shrink: remove unused file entries from base filesets
- Unknown blob files: blob files on disk with no matching record in the database
- Pack compaction: rewrite live pack entries and reclaim pack files with too much unused space
- Unknown pack files: pack files on disk with no matching record in the database

!!! note

    In most cases, you do not need to run this command manually; Prime Backup keeps the database clean during normal operations

### Manual Pack Compaction

Compact all pack files using a 100% live-size threshold:

```
!!pb database compact_packs
```

This rewrites pack files that contain unused space and removes pack files with no live entries left

Pack files whose entries are all still live are skipped

!!! note

    This command is intended for manual maintenance and debugging; for routine cleanup, use `!!pb database prune`

### SQLite Vacuum

Compact the SQLite database file to reclaim disk space:

```
!!pb database vacuum
```

Example output:

```
> !!pb database vacuum
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO]: [PB] Compacting database, minimizing the size of the database file, please wait...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO]: [PB] Database compaction complete, cost 2.34s, size change: 45.67MiB -> 32.15MiB (-13.52MiB) (70.40%)
```

!!! note

    Under the default configuration, Prime Backup runs SQLite vacuum periodically, so you rarely need to execute this command manually
