---
title: 'Handling Corrupted Data Files'
---

# Handling Corrupted Data Files

This document describes how to identify problems, temporarily bypass restoration failures, and repair corruption when data object files (blobs) or data chunk files (chunks) in the Prime Backup storage pool become corrupted

## Background

Prime Backup persists backup data to the file system in the form of data objects (Blobs). There are two storage methods:

- Direct (`direct`): each blob corresponds to an independent file, stored at `pb_files/blobs/{first 2 chars of hash}/{full hash}`,
  e.g.: `pb_files/blobs/8b/8b36d71e250e25527f59b8fd9e0f2dce`
- Chunked (`chunked`): large files are split into multiple data chunks (chunks) via the CDC algorithm, each chunk stored independently at `pb_files/blobs/_chunks/{first 2 chars of hash}/{full hash}`,
  e.g.: `pb_files/blobs/_chunks/3a/3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5`

When these files are corrupted or lost due to disk failures, file system errors, unexpected power loss, or accidental operations, PB will be unable to restore the corresponding files during a restoration (or export), causing the operation to fail

> For a detailed description of the storage structure, see [Storage Structure](../concept/storage_structure.md)

## Symptoms

### Restoration Failure

By default, PB performs a hash verification on each restored file during restoration (`verify_blob=True`). If a blob or chunk file is missing, corrupted, or its hash/size does not match the database record, the restoration aborts with an error and rolls back all written files

Scenario 1: Direct blob file corrupted (hash mismatch)

```
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e250e25527f59b8fd9e0f2dce, actual written 9f4c7b3e5d1a2c8e6f0b4d9e2a7f3c1d
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: Error occurs during export to directory, applying rollback
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Restore to backup #78 failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e250e25527f59b8fd9e0f2dce, actual written 9f4c7b3e5d1a2c8e6f0b4d9e2a7f3c1d
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: The server is left stopped due to the failure. Prime Backup will not attempt to restart it automatically
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: You can try to fix the issue and perform another restoration, or just start the server manually via MCDR command
```

Scenario 2: A chunk file of a chunked blob corrupted (hash mismatch)

```
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (chunk 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5), expected 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5, actual written ff92dcb10e3a7b5c2d4f8e1a9c6b0d3f
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: Error occurs during export to directory, applying rollback
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Restore to backup #78 failed: hash mismatched for world/region/r.0.0.mca (chunk 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5), expected 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5, actual written ff92dcb10e3a7b5c2d4f8e1a9c6b0d3f
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: The server is left stopped due to the failure. Prime Backup will not attempt to restart it automatically
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: You can try to fix the issue and perform another restoration, or just start the server manually via MCDR command
```

!!! warning

    After a restoration failure, the server process remains stopped and PB will not restart it automatically  
    You need to fix the issue first, or start the server manually via MCDR command: `!!MCDR start`

Since PB rolls back the restoration target directory to its original state upon failure, a failed restoration will not corrupt existing server files

## Scanning and Identifying the Problem

### Quick Check (`validate all`)

Use the following command to validate all components in the database at once:

```
!!pb database validate all
```

This command validates in sequence: blobs, chunk-related objects (chunks), files, filesets, and backups

Example output (all healthy):

```
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO]: [PB] Start validating blobs, please wait...
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:51] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] All 4325 blobs are healthy
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] Start validating chunks (chunks, chunk groups, relation bindings), please wait...
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 8765, validated 8765, ok 8765, bad 0
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO]: [PB] All 8765 chunks, ... chunk groups, ... + ... bindings are healthy
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO]: [PB] ...(files/filesets/backups validation omitted)...
[MCDR] [23:16:12] [PB@xxx-worker-heavy/INFO]: [PB] Validation done, cost 34.21s. blobs: good, chunks: good, files: good, filesets: good, backups: good
```

!!! note

    This document only covers blob/chunk file-level corruption handling (corresponding to the `blobs` and `chunks` validation results)
    For anomalies in `files`, `filesets`, `backups` and other components, these are database structure-level issues and are outside the scope of this document

### Scanning Direct Blob Files

Use the following command to perform a full scan of all data objects:

```
!!pb database validate blobs
```

Output when all is healthy:

```
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO]: [PB] Start validating blobs, please wait...
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:15:51] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] All 4325 blobs are healthy
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] Validation done, cost 15.32s. blobs: good
```

Output when anomalies are found (example):

```
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO]: [PB] Start validating blobs, please wait...
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4322, bad 3
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Validated 4325 / 4325 blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Found 3 / 4325 bad blobs in total
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Missing blob amount: 2
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 1. 4fc02ad5e7508949634f012fb96c2968: blob file /path/to/pb_files/blobs/4f/4fc02ad5e7508949634f012fb96c2968 does not exist
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 2. 54695a153daae89e685894ee966a277e: blob file /path/to/pb_files/blobs/54/54695a153daae89e685894ee966a277e does not exist
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Mismatched blob amount: 1
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 1. 8b36d71e250e25527f59b8fd9e0f2dce: stored size mismatch, expect 786432, found 786428
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Affected range: 6 / 9147 file objects, 2 / 26 filesets, 16 / 21 backups
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] See log file pb_files/logs/validate.log for details and affected stuffs of these bad blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] Validation done, cost 6.34s. blobs: bad
```

#### Anomaly Type Reference

| Type Key     | Name            | Description                                                                                                |
|--------------|-----------------|------------------------------------------------------------------------------------------------------------|
| `missing`    | Missing blob    | The blob file does not exist at its storage path                                                           |
| `corrupted`  | Corrupted blob  | The blob file exists but decompression fails                                                               |
| `mismatched` | Mismatched blob | The decompressed hash or size does not match the database record                                           |
| `bad_layout` | Bad-layout blob | The chunk group bindings of a chunked blob are abnormal                                                    |
| `invalid`    | Invalid blob    | A basic field of the blob (such as compress method) is illegal                                             |
| `orphan`     | Orphan blob     | The blob is not referenced by any file (does not affect restoration, can be cleaned with `database prune`) |

The first three (`missing`, `corrupted`, `mismatched`) are the most common file-level corruptions that directly cause restoration failures

#### Checking the Detailed Log

The end of the output shows the log file path (`pb_files/logs/validate.log`), which contains the full list of affected backup IDs and file samples — check this file first:

```
Affected file objects / total file objects: 6 / 9147
Affected file samples (len=3):
- FileInfo(fileset_id=12, path='world/region/r.0.0.mca', ...)
- FileInfo(fileset_id=15, path='world/region/r.0.0.mca', ...)
- FileInfo(fileset_id=18, path='world/region/r.0.0.mca', ...)
Affected backup / total backups: 16 / 21
Affected backup IDs (bad blobs): [3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33]
```

### Scanning Chunk Files of Chunked Blobs

Use the following command to scan all chunks and their associated objects (chunk groups, binding relationships):

```
!!pb database validate chunks
```

Example output when chunk corruption is found:

```
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO]: [PB] Start validating chunks (chunks, chunk groups, relation bindings), please wait...
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation start
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 8765 / 8765 chunks
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 8765, validated 8765, ok 8763, bad 2
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] Found 2 / 8765 bad chunks in total
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] Corrupted chunk file: 2
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 1. id=1234 hash=3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5: cannot read and decompress chunk file: (<class 'zlib.error'> Error -5 while decompressing data: incomplete or truncated stream
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 2. id=5678 hash=9b4c3d7e2f1a0b6c8e7d5c3a2b1f9e0d: cannot read and decompress chunk file: (<class 'zlib.error'> Error -5 while decompressing data: incomplete or truncated stream
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] Affected range: 3 / 9147 file objects, 1 / 26 filesets, 8 / 21 backups
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] See log file pb_files/logs/validate.log for details and affected stuffs of these bad blobs
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] Validation done, cost 4.56s. chunks: bad
```

Chunk corruption types are similar to blobs: `missing_file` (file missing), `corrupted` (decompression failed), `mismatched` (hash/size mismatch)

Since a chunk can be shared by multiple chunked blobs, a single corrupted chunk may affect multiple files across multiple backups

### Further Locating Affected Files and Backups

The `!!pb database inspect` series of commands can be used to drill down into affected objects:

Inspect a specific data object (blob):

```
!!pb database inspect blob 8b36d71e250e25527f59b8fd9e0f2dce
```

Example output:

```
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] ======== Blob 8b36d71e250e2552 ========
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] ID: 42
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Storage method: direct
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Hash: 8b36d71e250e25527f59b8fd9e0f2dce (xxh128)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Compress: zstd
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Raw size: 1048576 (1.00MiB)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Stored size: 786432 (768.00KiB)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] Associated file count: 6. Samples:
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 1. fileset 12, path: world/region/r.0.0.mca
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 2. fileset 15, path: world/region/r.0.0.mca
```

Inspect a specific file in a backup:

```
!!pb database inspect file 78 world/region/r.0.0.mca
```

Inspect a specific backup:

```
!!pb database inspect backup 78
```

## Temporary Bypass Measures

!!! warning

    The following measures are for temporary emergency use only and do not fix the underlying corruption. The backup content restored using these options will be incomplete, and the relevant game files (e.g., world saves) may be corrupted. Use with caution after fully understanding the risks

### Skip Failed Files (`--fail-soft`)

Add the `--fail-soft` flag to the restoration command, and PB will catch and log export errors for each file, skip the failed files, and continue the restoration:

```
!!pb back 78 --fail-soft
```

`--fail-soft` applies to all types of export failures (including missing files, decompression failures, and hash mismatches). After restoration completes, PB logs all skipped files:

```
[MCDR] [22:10:12] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e..., actual written 9f4c7b3e...
[MCDR] [22:10:12] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.1.0.mca' to path world/region/r.1.0.mca failed: hash mismatched for world/region/r.1.0.mca (chunk 3a7f29c4...), expected 3a7f29c4..., actual written ff92dcb1...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: Export done with 2 failures
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Found 2 failures during backup export
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: world/region/r.0.0.mca mode=0o100644: (VerificationError) hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e..., actual written 9f4c7b3e...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: world/region/r.1.0.mca mode=0o100644: (VerificationError) hash mismatched for world/region/r.1.0.mca (chunk 3a7f29c4...), expected 3a7f29c4..., actual written ff92dcb1...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restore to backup #78 done, cost 10.32s (backup 2.41s, restore 7.91s), starting the server
```

!!! warning

    When using `--fail-soft`, the final state of skipped files in the restoration directory depends on the error type:

    - Missing file (`missing`/`missing_file`): blob/chunk file does not exist and cannot be written; the file will be absent at the destination path
    - Corrupted file (`corrupted`): an exception occurs during decompression; the destination file may exist in a truncated/partially written state
    - Hash mismatch (`mismatched`): the file is fully written but the content comes from corrupted data, so the content is incorrect

    Additionally, since PB clears the restoration target directory at the start, the pre-restoration content of any failed file has already been deleted and cannot be automatically recovered. Manual restoration to a pre-restoration backup is required if one was created

### Skip Hash Verification (`--no-verify`)

Add the `--no-verify` flag to the restoration command, and PB will skip hash/size verification on restored files:

```
!!pb back 78 --no-verify
```

`--no-verify` applies to cases where the blob/chunk file content can be read and decompressed normally, but the hash or size does not match the database record (i.e., `mismatched` type)
For missing files or files that cannot be decompressed, `--no-verify` has no effect and must be used together with `--fail-soft`

The two flags can be combined:

```
!!pb back 78 --fail-soft --no-verify
```

## Repair Methods

### What Not to Do

!!! danger

    **Do not directly delete blob or chunk files**

    Directly deleting files under `pb_files/blobs/` or `pb_files/blobs/_chunks/` in the file system will cause the database records to become inconsistent with the file system
    PB has no knowledge of the deletion; subsequent restoration, export, and other operations will still attempt to read the non-existent files, leading to various hard-to-diagnose errors

!!! danger

    **Do not directly modify the SQLite database**

    The PrimeBackup database (`pb_files/prime_backup.db`) has a complex relational structure (multiple layers of associations including blob, chunk, chunk_group, binding, etc.)
    Manual modification can easily break referential integrity, putting the database into a corrupted state that is difficult to recover from

### Method 1: Restore blob/chunk Files from an External Backup

Applicable scenario: You have an external backup of the `pb_files` directory (e.g., an OS-level file backup) that contains intact blob or chunk files

Steps:

1. Identify the file paths to restore (obtain hashes from validate output):
   - Direct blob: `pb_files/blobs/{first 2 chars of hash}/{full hash}`  
     e.g.: `pb_files/blobs/8b/8b36d71e250e25527f59b8fd9e0f2dce`
   - Chunk file: `pb_files/blobs/_chunks/{first 2 chars of hash}/{full hash}`  
     e.g.: `pb_files/blobs/_chunks/3a/3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5`

2. Copy the corresponding file from the external backup to its original path, overwriting the corrupted file (or placing it there if the file is missing)

3. Run `!!pb database validate blobs` (or `validate chunks`) again to confirm the issue has been fixed

!!! tip

    Before restoring, use `!!pb database inspect blob <hash>` to confirm the blob's storage method (direct/chunked) and compression method, ensuring the external backup file matches

### Method 2: Delete the Affected Backups

Applicable scenario: The backups corresponding to the corrupted blob/chunk are no longer needed, or you prefer to discard these backups rather than keep incomplete data

Steps:

1. Obtain the list of affected backup IDs from the validate output or log file

2. Delete the affected backups one by one:

    ```
    !!pb delete <backup_id>
    ```

    For example, to delete backups #3, #5, #7:

    ```
    !!pb delete 3 5 7
    ```

    Or use range deletion (if affected backup IDs are consecutive):

    ```
    !!pb delete_range 3-7
    ```

3. After deleting backups, PB will automatically clean up orphan blobs and chunks no longer referenced by any backup (including deleting their physical files). If any remain, you can manually run:

    ```
    !!pb database prune
    ```

!!! note

    When deleting backups, if a blob/chunk is still referenced by other non-deleted backups, it will not be automatically deleted; the corrupted data still exists in the storage pool, just no longer affecting the deleted backups
    To completely remove corrupted data, you need to delete all backups that reference it

!!! tip

    Affected backup IDs can be clicked directly from the in-game validate output, or retrieved in full from the `validate.log` log file

### Method 3: (Not Yet Implemented) Provide Files with Intact Data to Rebuild blob/chunk

!!! info

    This feature has not yet been implemented

In theory, if you have one or more files with the same content as the damaged blob/chunk, a dedicated repair command could allow PB to re-ingest those files and fix the corrupted storage entries

The planned usage would be: provide one or more file paths (or an archive containing those files), PB computes the hash of each input file,
and searches the database for blob or chunk entries with a matching hash in a corrupted state (`missing`, `corrupted`, `mismatched`). If found, it re-writes the file to the corresponding storage path, completing the repair

The current version of PrimeBackup does not support this: the `!!pb import` command, when processing files, will not re-write the physical file if a blob record with the same hash already exists in the database,
so even importing a backup with the same content will not replace the corrupted blob file

Watch future versions for updates on this feature

## Summary

| Issue Type                                                 | Recommended Action                                                                                          |
|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| blob/chunk file missing (`missing`/`missing_file`)         | Restore the file from an external backup, or delete the affected backups                                    |
| blob/chunk file corrupted, cannot decompress (`corrupted`) | Restore the file from an external backup, or delete the affected backups                                    |
| Hash/size mismatch (`mismatched`)                          | Restore from external backup, or delete affected backups; temporarily use `--no-verify` to bypass           |
| Temporarily need to restore a corrupted backup             | `!!pb back <ID> --fail-soft` (skip corrupted files) or `--no-verify` (skip hash check)                      |
| Want to determine the scope of corruption                  | `!!pb database validate blobs` / `validate chunks` / `validate all`, and check `pb_files/logs/validate.log` |
