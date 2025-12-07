---
title: 'Database Maintenance'
---

Consistency check and Database Organization

## Database Validation

| Command                           | Description                                                                                        |
|-----------------------------------|----------------------------------------------------------------------------------------------------|
| `!!pb database validate blobs`    | Validate the correctness of blobs, e.g. data size, hash value                                      |
| `!!pb database validate files`    | Validate the correctness of file objects, e.g. the association between files and blobs             |
| `!!pb database validate filesets` | Validate the correctness of fileset objects, e.g. the association between filesets and their files |
| `!!pb database validate backups`  | Validate the correctness of backup objects, e.g. the association between backups and filesets      |
| `!!pb database validate all`      | Validate all of the above                                                                          |

Example:

```
!!pb database validate all
```

Example output:

```
> !!pb database validate all
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO]: [PB] Start validating blobs, please wait...
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:16:17] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:16:20] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] All 4325 blobs are healthy
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
[MCDR] [23:16:21] [PB@fc91-worker-heavy/INFO]: [PB] Validation done, cost 3.39s. blobs: good, files: good, filesets: good, backups: good
```

### Validation Content

Blobs:

- File Existence: Check if the blob files exist in storage
- Integrity: Verify the hash values of the blob files
- Size Matching: Check if the stored size matches the record
- Compression Validation: Verify the integrity of compressed data

Files:

- Reference Integrity: Check if the blobs referenced by files exist
- Fileset Association: Verify the association between files and filesets
- Metadata Consistency: Check if file metadata is complete

Filesets:

- Backup Association: Verify the association between filesets and backups
- File Count: Check the number of files in the fileset
- Size Calculation: Verify the correctness of fileset size calculation

Backups:

- Fileset Reference: Check if the filesets referenced by backups exist
- Timestamp Order: Verify the order of backup timestamps
- Metadata Integrity: Check if backup metadata is complete


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


## Database Cleanup

### Invalid Data Cleanup

Prune useless stuffs as orphaned objects in the database

```
!!pb database prune
```

Stuffs to be pruned

- Orphaned Blobs: Blobs no longer referenced by any files
- Orphaned Files: Files no longer referenced by any filesets
- Orphaned Filesets: Filesets no longer referenced by any backups
- Base Fileset Compression: Optimize the storage structure of base filesets
- Unknown Blob Files: Blob files that exist in file system but have no records in the database

!!! note

    In most cases, you do not need to manually execute this command. Prime Backup ensures the database remains in a clean state during daily operations

### SQLite Vacuum

Compact the SQLite database file to reduce disk usage

```
!!pb database vacuum
```

Example output:

```
> !!pb database vacuum
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始压缩数据库...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 压缩完成: 耗时 2.34s, 大小 45.67MiB -> 32.15MiB (-13.52MiB, 70.4%)
```

!!! note

    Under the default configuration, Prime Backup will automatically perform SQLite vacuum tasks periodically, so in most cases, you do not need to manually execute this command
