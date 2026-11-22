---
title: 'Backup Display'
---

View backups

## Backup List

### Basic Usage

To view the backup list, use the following command:

```
!!pb list
```

This command will display the most recent 10 backups, sorted in descending order by time

Example output:

```
> !!pb list
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] ======== Backup List ========
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] There are 17 backups:
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#85] 162.45MiB 2025-01-31 23:59:59: Final backup of January
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#84] 159.82MiB 2025-01-30 16:45:30: Building completed
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#83] 157.36MiB 2025-01-28 12:15:45: Storage restructure 2
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#82] 154.91MiB 2025-01-25 09:30:20: Storage restructure 1
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#81] 152.47MiB 2025-01-22 14:00:00: Weekly backup
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#80] 150.03MiB 2025-01-20 20:18:33: no comment
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#79] 147.59MiB 2025-01-18 11:05:12: New terrain
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#78] 145.15MiB 2025-01-15 08:22:45: Started filling pit
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#77] 142.71MiB 2025-01-10 02:00:00: no comment
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] [#76] 140.27MiB 2025-01-05 19:40:15: Base expansion completed
[MCDR] [18:22:40] [PB@fc91-worker-light/INFO]: [PB] <- 1/2 ->
```

### List Format

Each backup entry is displayed in the following format:

```
[#ID] [>] [x] [flags] size creation_time: comment
```

- `[#ID]`: backup ID, click to view details
- `[>]`: green restore button, shown only in in-game chat, click to restore quickly
- `[x]`: red delete button, shown only in in-game chat, click to delete the backup. Protected backups are shown in gray
- `[flags]`: backup flags, shown only when `--flags` is passed
- `size`: raw backup size before deduplication and compression
- `creation_time`: time when the backup was created
- `comment`: backup comment text (shown as gray "no comment" if empty)

## Optional Parameters

The `!!pb list` command supports the following optional parameters:

| Parameter             | Description                                        |
|-----------------------|----------------------------------------------------|
| `[page]`              | View backups on the specified page                 |
| `--per-page <count>`  | Number of backups per page (1-1000)                |
| `--sort <order>`      | Sorting order (`id`, `id_r`, `time`, `time_r`)     |
| `--creator <creator>` | Show only backups created by the specified creator |
| `--me`                | Show only backups created by yourself              |
| `--from <start_date>` | Show only backups created after the given date     |
| `--to <end_date>`     | Show only backups created before the given date    |
| `-a, --all`           | Show all backups (including hidden and temporary)  |
| `--flag, --flags`     | Show backup flags derived from backup tags         |

Sorting order parameter supports the following values:

- `id`: Sort by backup ID ascending
- `id_r`: Sort by backup ID descending
- `time`: Sort by creation time ascending
- `time_r`: Sort by creation time descending (default)

Date parameters support the following formats:

| Format                 | Example                      | Description                                   |
|------------------------|------------------------------|-----------------------------------------------|
| `%Y`                   | `2023`                       | year                                          |
| `%Y%m`                 | `202311`                     | year-month                                    |
| `%Y%m%d`               | `20231130`                   | year-month-day                                |
| `%Y-%m-%d`             | `2023-11-30`                 | year-month-day                                |
| `%Y/%m/%d`             | `2023/11/30`                 | year/month/day                                |
| `%Y%m%d%H`             | `2023113021`                 | year-month-day-hour                           |
| `%Y%m%d%H%M`           | `202311302139`               | year-month-day-hour-minute                    |
| `%Y%m%d%H%M%S`         | `20231130213955`             | year-month-day-hour-minute-second             |
| `%Y%m%d %H%M%S`        | `20231130 213955`            | year-month-day hour-minute-second             |
| `%Y%m%d %H:%M:%S`      | `20231130 21:39:55`          | year-month-day hour:minute:second             |
| `%Y-%m-%d %H:%M:%S`    | `2023-11-30 21:39:55`        | year-month-day hour:minute:second             |
| `%Y-%m-%d %H:%M:%S.%f` | `2023-11-30 21:39:55.123456` | year-month-day hour:minute:second.microsecond |

## Backup Details

View detailed information for a specific backup:

```
!!pb show 45
```

Example output:

```
> !!pb show 45
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] ======== Backup #45 ========
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Creation date: 2025-08-24 23:50:07
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Comment: 2
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Size (stored): 64.62MiB (54.6%)
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Size (raw): 118.39MiB
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Creator: Console
[MCDR] [01:42:27] [PB@f133-worker-light/INFO]: [PB] Tags: empty
```

## Backup Flags

When using the `--flags` parameter, the backup list will display flags:

```
!!pb list --flags
```

Flag descriptions:

| Flag | Description        |
|------|--------------------|
| `H`  | Hidden backup      |
| `T`  | Temporary backup   |
| `P`  | Protected backup   |
| `S`  | Scheduled backup   |

Example:

```
[#65] [>] [x] H-P- 119.26MiB 2025-01-02 14:00:00: Secret backup
[#62] [>] [x] ---S 118.39MiB 2025-01-02 12:00:00: Scheduled backup
[#58] [>] [x] -T-- 118.39MiB 2025-01-02 11:00:00: Automatic backup before restoring to #48
```

## Practical Commands

View the most recent 20 backups

```
!!pb list --per-page 20
```

View backups from specific creators

```
!!pb list --creator console:
!!pb list --creator player:Fallen_Breath
```

View backups within specific time range

```
!!pb list --from 20240101 --to 20240131
```

View all backups (including hidden and temporary backups)

```
!!pb list --all --flags
```

View backups sorted by ID ascending

```
!!pb list --sort id
```
