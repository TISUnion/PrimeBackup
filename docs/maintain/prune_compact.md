---
title: 'Prune & Compact Concepts Explained'
---

# Prune & Compact Concepts Explained

Prime Backup involves multiple "housekeeping" concepts (prune / compact / vacuum), each operating at a different layer, triggered at different times, and producing different effects

This page provides a side-by-side overview of these concepts, focusing on their differences to help administrators get a clear picture at a glance

## Overview

| Concept             | Target               | Auto-triggered                               | Manual Command                |
|---------------------|----------------------|----------------------------------------------|-------------------------------|
| Backup Prune        | Backups              | Yes (scheduled job)                          | `!!pb prune`                  |
| Database Prune      | Composite operation  | No                                           | `!!pb database prune`         |
| Pack Compaction     | Pack files           | Yes (on chunk deletion / scheduled job)      | `!!pb database compact_packs` |
| SQLite Vacuum       | Database file        | Yes (scheduled job)                          | `!!pb database vacuum`        |
| Base Fileset Shrink | Base filesets        | Yes (on backup deletion)                     | (included in Database Prune)  |
| Orphan Object Scan  | Database objects     | No                                           | (included in Database Prune)  |
| Unknown File Scan   | Storage directories  | No                                           | (included in Database Prune)  |

---

## Backup Prune

Operates at the backup layer, deleting excess old backups according to the retention policy

### What It Does

Deletes backups that no longer need to be retained one by one according to the configured retention policy, and cascades to release the database objects and physical storage exclusively owned by those backups

### Scope

Backups are divided into three categories by tag and evaluated separately: regular, scheduled, and temporary

Each category independently applies its own `PruneSetting` configuration; backups with the protection tag (`is_protected = true`) are never pruned regardless

### Retention Decision Process

1. Use `last`, `hour`, `day`, `week`, `month`, `year` to select a representative backup from each time bucket according to the [PBS retention policy](https://pbs.proxmox.com/docs/prune-simulator/) and mark it for retention
2. Among the backups marked in step 1, further eliminate those that are excessive or expired using `max_amount` (retention cap) and `max_lifetime` (maximum lifetime)
3. Delete all backups that were not marked for retention

The detailed deletion decisions are logged to `pb_files/logs/prune.log`

### How to Trigger

- Automatic: scheduled job `prune_backup`, triggered according to `prune.interval` (default `6h`) or `prune.crontab`
- Manual: `!!pb prune` (requires permission level 3)

---

## Database Prune

Operates across multiple storage layers, performing a full bottom-level cleanup in one run

### What It Does

This is a composite command that executes all of the following cleanup steps in order:

1. Orphan Object Scan
2. Base Fileset Shrink
3. Unknown Blob File Scan
4. Pack Compaction, using `backup.pack_maintenance_compact_threshold`
5. Unknown Pack File Scan

### How to Trigger

- Manual: `!!pb database prune` (requires permission level 4)

!!! note

    Prime Backup already cleans up the corresponding data promptly during routine operations (such as deleting backups and pruning), so this command rarely needs to be run manually

---

## Pack Compaction

Operates at the pack file layer, eliminating "dead space" in pack files by rewriting them

### What It Does

Pack files are written in an append-only fashion; when chunks are deleted, the byte ranges they occupied are not immediately reclaimed and become dead space

During compaction, pack files whose dead ratio exceeds the threshold have their live entries rewritten into new pack files, and the old files are then deleted; pack files where all entries are dead are deleted directly

### Thresholds

- `backup.pack_auto_compact_threshold` (default `0.5`): the minimum live ratio used when triggering immediately; if the live data in the affected pack file falls below this ratio, compaction runs right away
- `backup.pack_maintenance_compact_threshold` (default `0.8`): the minimum live ratio used by maintenance tasks (scheduled jobs and `database prune`); the looser threshold means more pack files get compacted

### How to Trigger

| Trigger Scenario                      | Details                                                                                                          |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------|
| After a chunk is deleted (immediate)  | If the affected pack file's live ratio falls below `pack_auto_compact_threshold`, compaction runs immediately    |
| Scheduled job `compact_pack`          | Default crontab `0 5 * * 0` (every Sunday at 05:00), uses `pack_maintenance_compact_threshold`                   |
| `!!pb database prune` step 4          | Uses `pack_maintenance_compact_threshold`                                                                        |
| `!!pb database compact_packs`         | Threshold fixed at `1.0`, only skips fully-live pack files (requires permission level 4)                         |

---

## SQLite Vacuum

Operates at the database file layer, defragmenting the SQLite database file itself

### What It Does

SQLite does not shrink the database file immediately after deleting data, instead leaving holes in place; the `VACUUM` command rebuilds the database file, eliminating holes and defragmenting it to reduce disk usage

This operation does not modify any backup data or storage objects; it only affects the file size of `prime_backup.db` itself

### How to Trigger

- Automatic: scheduled job `vacuum_sqlite`, default crontab `0 7 * * 0` (every Sunday at 07:00)
- Manual: `!!pb database vacuum` (requires permission level 4)

---

## Base Fileset Shrink

Operates at the fileset layer, removing redundant file entries from base filesets

### What It Does

Filesets use a base + delta structure; a file entry in the base fileset is considered redundant if it has been completely overridden or deleted by all delta filesets that reference it

The shrink operation will:

- Remove these redundant file entries from the base fileset
- Reclassify delta entries originally marked as "override (delta_override)" to "add (delta_add)" so they can survive independently going forward
- Remove delta entries originally marked as "delete (delta_remove)", since the base entry they referenced no longer exists and the delete marker becomes meaningless

### How to Trigger

- Automatic: when deleting a backup, if the base fileset it belongs to is still shared by other backups, a shrink is automatically performed on that base fileset
- Indirect: `!!pb database prune` step 2 (`ShrinkAllBaseFilesetsAction`, scans all base filesets)

!!! note

    The routine backup deletion process already cleans up redundant file entries in filesets in a timely manner, so this scan normally finds no redundant objects

---

## Orphan Object Scan & Delete

Operates at the database object layer, scanning and deleting "orphaned" database records that are no longer referenced by any parent object

### Source of Orphan Objects

The normal deletion process cascades to clean up the corresponding objects, but in rare circumstances (such as unexpected interruptions or concurrency anomalies) orphan objects may be left behind

### What Gets Cleaned

The following objects are scanned and deleted in order:

| Object             | Criteria                                                                                                          |
|--------------------|-------------------------------------------------------------------------------------------------------------------|
| Orphan Fileset     | A fileset not referenced by any backup                                                                            |
| Orphan File        | A file object not referenced by any fileset                                                                       |
| Orphan Blob        | A blob not referenced by any file                                                                                 |
| Orphan Chunk Group | A chunk group not referenced by any blob                                                                          |
| Orphan Chunk       | A chunk not referenced by any chunk group                                                                         |
| Orphan Binding     | Applies to the Blob-ChunkGroup and ChunkGroup-Chunk binding tables; removes rows pointing to non-existent objects |

### How to Trigger

- Indirect: `!!pb database prune` step 1

!!! note

    The routine backup deletion process already cleans up the corresponding orphan objects in a timely manner, so this scan normally finds no orphan objects

---

## Unknown File Scan & Delete

Operates at the filesystem layer, scanning storage directories and deleting files that have no corresponding database record

### What It Does

Prime Backup may leave temporary files in storage directories when a restore fails or is unexpectedly interrupted; these files have no database record and are not cleaned up by normal operations

### Scan Scope

| Storage Directory   | Details                                                                            |
|---------------------|------------------------------------------------------------------------------------|
| `pb_files/blobs/`   | Scans the direct blob storage directory and deletes files absent from the database |
| `pb_files/packs/`   | Scans the pack file storage directory and deletes files absent from the database   |

### How to Trigger

- Indirect: `!!pb database prune` steps 3 and 5
