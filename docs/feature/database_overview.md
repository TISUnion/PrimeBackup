---
title: 'Database Overview'
---

View database statistics overview, used to quickly understand the current status of the Prime Backup database

## Command

```
!!pb database overview
```

## Function

View overall statistical information of Prime Backup database

## Example Output

```
> !!pb database overview
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] ======== Database overview ========
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Database version: 4
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Database file size: 2.67MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Hash method: blake3
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [Backup]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Backup count: 22
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Backup storage actual size sum: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Backup storage uncompressed size sum: 657.79MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Total raw size of all backup files: 2.50GiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Overall compression ratio (dedup + compress): 17.6%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [File]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Fileset count: 26
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] File count: 70469 (9190 objects)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] File object dedup stats: count -86.95%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [Blob]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob count: 4339
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob stored size sum: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob raw size sum: 657.79MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob dedup stats: count -52.79%, size -74.31%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [Chunk]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Chunk count: 1280
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Chunk stored size sum: 190.23MiB (74.1%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Chunk raw size sum: 256.82MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Chunk dedup stats: count -31.44%, size -57.62%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [Pack]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Pack count: 16
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Pack file size sum: 190.23MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Pack live size sum: 176.91MiB (93.0%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Pack live entry count: 1280
```

## Output Sections

- Backup: high-level backup count plus the total payload currently stored as direct blobs and pack files
- File: fileset and file statistics for the logical backup contents
- Blob: statistics for blob objects
- Chunk: statistics for chunk objects
- Pack: statistics for pack files and live pack entries
