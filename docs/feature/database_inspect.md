---
title: 'Database Internal Object Inspection'
---

View and inspect object information in the database.

## Overview

Prime Backup's database inspection functionality allows you to deeply examine various components of the backup system, including backups, filesets, files, and blobs

## Backup Inspection

### View Backup Details

View complete information for a specific backup:

```
!!pb database inspect backup <backup_id>
```

Example:

```
!!pb database inspect backup 45
```

Example output:

```
> !!pb database inspect backup 45
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] ======== Backup #45 ========
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] ID: 45
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Timestamp (microsecond): 1756050607173147 (date: 2025-08-24 23:50:07)
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Creator: "console:" (Console)
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Comment: "2"
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Backup targets: world, world15, world21
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Tags: {}
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Fileset: base 39, delta 54
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Raw size: 124137302 (118.39MiB)
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] Stored size: 67760442 (64.62MiB)
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] File counts: 4106
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] - Regular files: 4068
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] - Directories: 38
[MCDR] [01:58:56] [PB@f133-worker-light/INFO]: [PB] - Symlinks: 0
```

## File Inspection

### View Files in Backup

View detailed information for a specific file in a backup:

```
!!pb database inspect file <backup_id> <file_path>
```

Example:

```
!!pb database inspect file 45 world/level.dat
```

Example output:

```
> !!pb database inspect file 45 world/level.dat
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] ======== File level.dat in fileset 39 ========
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Associated fileset: 39
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Path: world/level.dat
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Role: Standalone
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Mode: 33206 (-rw-rw-rw-)
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Blob hash: 86c7b6e480e869effd9200abe045b3db
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Blob compress: zstd
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Blob raw size: 1528 (1.49KiB)
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Blob stored size: 1537 (1.50KiB)
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Uid: 0
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Gid: 0
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Modify time: 1731254748636839 (2024-11-11 00:05:48.636839)
[MCDR] [01:41:24] [PB@f133-worker-light/INFO]: [PB] Backup containing this file: 3 (samples: #45, #43, #42)
```

### View Files in Fileset

View detailed information for a specific file in a fileset:

```
!!pb database inspect file2 <fileset_id> <file_path>
```

Displays the same content as above.

## Fileset Inspection

### View Fileset Details

View complete information for a specific fileset:

```
!!pb database inspect fileset <fileset_id>
```

Example:

```
!!pb database inspect fileset 87
```

Example output:

```
> !!pb database inspect fileset 87
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] ======== Fileset 87 ========
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] ID: 87
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] Kind: Delta fileset
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] File object count: 43
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] File count (delta): 22
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] Raw size (delta): 23620147 (22.53MiB)
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] Stored size (delta): 19735961 (18.82MiB)
[MCDR] [01:59:36] [PB@f133-worker-light/INFO]: [PB] Associated backup count: 1 (samples: #77)
```

## Blob Inspection

### View Blob Details

View complete information for a specific blob:

```
!!pb database inspect blob <hash>
```

The parameter `<hash>` can be a prefix of the complete hash string, as long as it uniquely identifies the object.

Example:

```
!!pb database inspect blob 8a3d
!!pb database inspect blob 8a3d32b705a1274850798ae26ff56ba9
```

Example output:

```
> !!pb database inspect blob 8a3d
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] ======== Blob 8a3d32b705a12748 ========
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] Hash: 8a3d32b705a1274850798ae26ff56ba9
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] Compress: zstd
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] Raw size: 736 (736.00B)
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] Stored size: 745 (745.00B)
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] Associated file count: 2. Samples:
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] 1. fileset 39, path: world15/playerdata/6ccb4484-ca33-41ee-a809-fece13a26c21.dat
[MCDR] [01:59:44] [PB@f133-worker-light/INFO]: [PB] 2. fileset 56, path: world15/playerdata/6ccb4484-ca33-41ee-a809-fece13a26c21.dat
```

If the provided hash is too short and cannot uniquely identify a blob, it will output:

```
> !!pb database inspect blob 8a
[MCDR] [02:00:06] [PB@f133-worker-light/INFO]: [PB] The given hash 8a cannot uniquely identify a blob
[MCDR] [02:00:06] [PB@f133-worker-light/INFO]: [PB] Found at least 3 candidates: 8a92569c48895c60bf2977f1be9591f4, 8abd332ba5b579ca00d0d54e50b8bedd, 8ad9bfd69fa3e84b15e2aa803cac0e4c
```