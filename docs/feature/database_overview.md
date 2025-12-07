---
title: 'Database Overview'
---

View database statistics overview, used to quickly understand the information status of Prime Backup database

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
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Database version: 3
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Database file size: 2.67MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Hash method: xxh128
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Backup count: 22
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] File count: 70469 (9190 objects)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] File raw size sum: 2.50GiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob count: 4339
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob stored size sum: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] Blob raw size sum: 657.79MiB
```
