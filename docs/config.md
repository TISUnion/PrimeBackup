---
title: 'Configuration'
---

## Configuration Explanation

Here's a full explanation on the config file, if you want to have a deep dive on it

### Root config

This is the root json object in the config files

```json
{
    "enabled": true,
    "debug": false,
    "storage_root": "./pb_files",
    "concurrency": 1,
    
    // Subconfigs. See the following sections
    "command": {/* Command config */},
    "server": {/* Server config */},
    "backup": {/* Backup config */},
    "scheduled_backup": {/* Scheduled backup config */},
    "prune": {/* Prune config */},
    "database": {/* Database config */}
}
```

#### enabled

The switch of the plugin. Set to false to disable the plugin

- Type: `bool`
- Default: `false`

#### debug

The debug switch. Set to true to enable debug logging

- Type: `bool`
- Default: `false`

#### storage_root

The root directory for Prime Backup to store all data files

The path is related to the work directory of MCDR. By default, the root directory will be `/path/to/mcdr_root/pb_files`

- Type: `str`
- Default: `"./pb_files"`

#### concurrency

The maximum concurrency to be used during all task and action executions

Setting the concurrency to a higher value (e.g. `4`) can speed up actions, such as backup creation and restoration,
but it will also consume more CPU during these actions

A value of `0` means using 50% of the CPU

- Type: `int`
- Default: `1`

---

### Command config

```json
{
    "prefix": "!!pb",
    "permission": {
        "root": 0,
        "abort": 1,  
        "back": 2,
        "confirm": 1,
        "database": 4,
        "delete": 2,
        "delete_range": 3,
        "export": 4,
        "list": 1,
        "make": 1,
        "prune": 3,
        "rename": 2,
        "show": 1,
        "tag": 3
    },
    "confirm_time_wait": "1m",
    "backup_on_restore": true,
    "restore_countdown_sec": 10
}
```

#### prefix

The prefix of all Prime Backup commands in MCDR. Normally you don't need to change it

- Type: `str`
- Default: `"!!pb"`

#### permission

Minimum required [MCDR permission level](https://mcdreforged.readthedocs.io/en/latest/permission.html) for all subcommands

It's a mapping that maps from string to integer, storing permission level requirements for all subcommands.
If a subcommand is not in the mapping, it will use level 1 as the default permission requirement

For example, in the default config, `"back"` is mapped to `2`, 
which means that the `!!pb back` command requires permission level >= 2 to execute

Specially, `"root"` refers to the root command, which is usually `!!pb`

- Type: `Dict[str, int]`

#### confirm_time_wait

Some of the MCDR commands requires user to enter `!!pb confirm` to continue.
This config defines the maximum time wait for those commands

When time exceeds, the command will be cancelled

- Type: [`Duration`](#duration)
- Default: `"1m"`

#### backup_on_restore

If an automatically backup should be made before restoring the world to a given backup. These automatic pre-restore backups are temporary backups

This is a safeguard prepared for those idiot users

- Type: `bool`
- Default: `true`

#### restore_countdown_sec

The duration in seconds of the countdown during backup restoring, before closing the Minecraft server

- Type: `int`
- Default: `10`

---

### Server config

Options on how to interact with the Minecraft server

```json
{
    "turn_off_auto_save": true,
    "commands": {
        "save_all_worlds": "save-all flush",
        "auto_save_off": "save-off",
        "auto_save_on": "save-on"
    },
    "saved_world_regex": [
        "Saved the game",
        "Saved the world"
    ],
    "save_world_max_wait": "10m"
}
```

#### turn_off_auto_save

If the autosave should be turned off before making a backup

Since turning off autosave provide a consistent view to the Minecraft world files, it's suggested to set this option to `true`

- Type: `bool`
- Default: `true`

#### commands

A set of Minecraft commands to be used by Prime Backup. Current they're only used during a backup creation

Timing of what Prime Backup will do during a backup creation

1. Turn off autosave with command stored in subconfig `auto_save_off`, if config [`turn_off_auto_save`](#turn_off_auto_save) is true and `auto_save_off` is a non-empty string
2. Save all worlds with command stored in subconfig `save_all_worlds`, if `save_all_worlds` is a non-empty string
3. Wait until all words are saved, i.e. the server outputs matches one of the regex in config [`saved_world_regex`](#saved_world_regex). If `saved_world_regex` is empty, then not waiting will be performed 
4. Create the backup
5. Turn on autosave with command stored in subconfig `auto_save_on`, if config [`turn_off_auto_save`](#turn_off_auto_save) is true and `auto_save_on` is a non-empty string

#### saved_world_regex

A list of regular expressions for identifying whether the server has already saved the world

!!! note

    The regex patterns will perform a [full match](https://docs.python.org/3/library/re.html#re.fullmatch) check on the server info content

- Type: `List[re.Pattern]`

#### save_world_max_wait

The maximum waiting time to before world saving is complete during backup creation

- Type: [`Duration`](#duration)
- Default: `"10m"`

---

### Backup config

Configs on how the backup is made

```json
{
    "source_root": "./server",
    "source_root_use_mcdr_working_directory": false,
    "targets": [
        "world"
    ],

    "ignored_files": [],
    "ignore_patterns": [
       "**/session.lock"
    ],
    "follow_target_symlink": false,
    "reuse_stat_unchanged_file": false,
	"creation_skip_missing_file": false,
	"creation_skip_missing_file_patterns": [
       "**"
    ],

    "hash_method": "xxh128",
    "compress_method": "zstd",
    "compress_threshold": 64,

	"fileset_allocate_lookback_count": 2
}
```

#### source_root

The root directory where the backup / restore operations happen

Usually this should be the same as the server [working_directory](https://mcdreforged.readthedocs.io/en/latest/configuration.html#working-directory) option of MCDR, 
i.e. the `server` directory by default

- Type: `str`
- Default: `"./server"`

#### source_root_use_mcdr_working_directory

If set to `true`, use the server working directory of MCDR as the value of [source_root](#source_root),
and the value of key [source_root](#source_root) in the config will be ignored

- Type: `bool`
- Default: `false`

#### targets

The target files / directories to make backups. They can also be [gitignore flavor](http://git-scm.com/docs/gitignore) patterns 

Usually you need to add the name(s) of your world folder(s) here

For example, for bukkit-like servers that split the world dimensions, you might want to use:

```json
"targets": [
    "world",
    "world_nether",
    "world_the_end"
]
```

Or use this wildcard pattern that matches everything whose name starts with "world" in the [source_root](#source_root) directory

```json
"targets": [
    "world*"
]
```

- Type: `List[str]`

Changelog:

- v1.10.0: Support gitignore flavor patterns

#### ignored_files

!!! warning

    Deprecated since v1.8.0. Use [ignore_patterns](#ignore_patterns) instead

!!! danger

    Files matched by `ignored_files` are also not considered during restore.  
    After the restore, only those files that were backed up will exist in the target folders.  
    Only use this option for useless files.

A list of file / directory names to be ignored during backup

If the name string starts with `*`, then it will ignore files with name ending with specific string, 
e.g. `*.test` makes all files ends with `.test` be ignored, like `a.test`

If the name string ends with `*`, then it will ignore files with name starting with specific string, 
e.g. `temp*`  makes all files starts with `temp` be ignored, like `tempfile`

- Type: `List[str]`

#### ignore_patterns

!!! danger

    Files matched by `ignore_patterns` are also not considered during restore.  
    After the restore, only those files that were backed up will exist in the target folders.  
    Only use this option for useless files.

A list of [gitignore flavor](http://git-scm.com/docs/gitignore) patterns for matching files / directories to be excluded during the backup

The root path for the pattern matching is [source_root](#source_root).
For example, if `source_root` is `server`, then pattern `world/trash*.obj` will match `server/world/trash1.obj`

It contains a `**/session.lock` pattern by default, which matches files named `session.lock` in any location.
It's used to solve the backup failure problem caused by `session.lock` being occupied by the server in Windows OS

- Type: `List[str]`

#### follow_target_symlink

When set to `true`, for [backup targets](#targets) that are symbolic links,
Prime Backup will not only create backups for them,
but also include the link targets of the symbolic links in the backup targets.

For example, in the following symbolic link graph:

```
world --> foo --> bar
^
backup target
```

Prime Backup will save not only the `world` symbolic link, but also the `foo` symbolic link and the final `bar` directory

- Type: `bool`
- Default: `false`

#### reuse_stat_unchanged_file

When enabled, during backup creation, Prime Backup will try to directly reuse files from the previous backup 
whose attributes (size, mtime, mode, etc.) have not changed. No file hash checking will be done on these attributes-unchanged files

Prime Backup will check the following file attributes. Files with the exact same attributes will be considered unchanged:

- File path
- File size
- File mode bits
- File owner's UID and GID
- File modification time (precision: microseconds)

If you want the maximum possible backup creation speed, you can try enabling this option.
However, this also introduces the potential risk of incomplete backups

!!! warning

    Please only enable this option after ensuring that the server's operating system and file system are functioning properly and stably. 
    Otherwise, if issues such as system time rollback or abnormal file system metadata occur, some of the files might 
    have their content changed but keep their stat unchanged, and then Prime Backup will create an incomplete backup

!!! tip

    Unless you really need this backup speed boost or the system disk read performance is too low, it is not recommended to enable this option.
    Prime Backup is already fast enough

- Type: `bool`
- Default: `false`

#### creation_skip_missing_file

In certain scenarios, server plugins / mods do not respect to the `/save off` command,
and may still delete files when PB is creating a backup.
These missing files can cause the backup operation to fail due to file not found errors

If you want to ignore such "file not found" errors in this scenario, you can enable this option

See also: the [creation_skip_missing_file_patterns](#creation_skip_missing_file_patterns) option,
which controls the scope of files for which "file not found" errors can be ignored

- Type: `bool`
- Default: `false`

#### creation_skip_missing_file_patterns

A list of [gitignore flavor](http://git-scm.com/docs/gitignore) pattern strings
used in combination with [creation_skip_missing_file](#creation_skip_missing_file) option

During backup creation, any "file not found" errors occurring for files matched by this option will be ignored

The default value is `["**"]`, which matches everything. It's suggested to limit it to those volatile files only, e.g. `["trash/*.tmp"]`

- Type: `List[str]`

#### hash_method

The algorithm to hash the files. Available options: `"xxh128"`, `"sha256"`, `"blake3"`

| Hash Method                                       | Description                                                                                                                                                       | Speed | cryptographically secure |
|---------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|--------------------------|
| [`xxh128`](https://github.com/Cyan4973/xxHash)    | A extremely fast, high-quality 128bit non-cryptographic hash algorithm. Recommend to use, unless you want theoretic extreme safety on hackers                     | ★★★★★ | :cross_mark:             |
| [`sha256`](https://en.wikipedia.org/wiki/SHA-2)   | A cryptographically secure and widely used 256bit hash algorithm. It's slower than xxh128, but the speed could be acceptable using cpu with hardware acceleration | ★★    | :check_mark:             |
| [`blake3`](https://github.com/BLAKE3-team/BLAKE3) | A cryptographically secure and speedy 256bit hash algorithm. It's still slower than xxh128, but is much faster than sha256                                        | ★★★☆  | :check_mark:             |

!!! note

    If you want to use `blake3` as the hash method, you need to install the `blake3` python library manually.
    It's not included in the default requirement list, because in some environments it might require rust runtime to build and install

    ```bash
    pip3 install blake3
    ```

- Type: `str`
- Default: `"xxh128"`

#### compress_method

The method to compress files stored in a backup

| Compress Method | Description                                                                                                                                                   | Speed | Compress rate |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|---------------|
| `plain`         | No compression, copy the file directly. Can utilize the copy-on-write technique if the file system supports that                                              | ★★★★★ | ☆             |
| `gzip`          | The [gzip](https://docs.python.org/3/library/gzip.html) library based on [zlib](https://www.zlib.net/). The format same as a `.gz` file                       | ★★    | ★★★★          |
| `lzma`          | The [LZMA](https://docs.python.org/3/library/lzma.html) algorithm. The same format as a `.xz` file. Provides the best compression, but the speed is very slow | ☆     | ★★★★★         |
| `zstd`          | The [Zstandard](https://github.com/facebook/zstd) algorithm. A good balance between speed and compression rate. Recommend to  use                             | ★★★☆  | ★★★★          |
| `lz4`           | The [LZ4](https://github.com/lz4/lz4) algorithm. Faster than Zstandard, and even much faster in decompression, but with a lower compression rate              | ★★★★  | ★★☆           |

!!! warning

    Changing `compress_method` will only affect new files in new backups (i.e., new blobs)

!!! note

    If you want to use `lz4` as the compress method, you need to install the `lz4` python library manually

    ```bash
    pip3 install lz4
    ```

- Type: `str`
- Default: `"zstd"`

#### compress_threshold

For files with a size less than the `compress_threshold`, no compression will be applied. They will be stored in the `plain` format.

!!! warning

    Changing `compress_threshold` will only affect new files in new backups (i.e., new blobs)

- Type: int
- Default: `64`

---

### Scheduled backup config

Configuration of the scheduled backup feature from Prime Backup

It's for creating a backup periodically and automatically for your server

```json
{
    "enabled": false,
    "interval": "12h",
    "crontab": null,
    "jitter": "10s",
    "reset_timer_on_backup": true,
    "require_online_players": false,
    "require_online_players_blacklist": []
}
```

#### enabled, interval, crontab, jitter

See the [crontab job setting](#crontab-job-setting) section

#### reset_timer_on_backup

If the schedule timer should be reset on each manual backup

This feature is only effective when the job is in interval mode

- Type: `bool`
- Default: `true`

#### require_online_players

If set to `true`, scheduled backups will only run normally when players are present on the server.
If all players logged out, then after the next scheduled backup is created, no more scheduled backup will be created

Example timeline:

```mermaid
flowchart LR
    subgraph g1[Players online]
    b1[8:00\ncreated] --> b2
    end

    subgraph g2[No players online]
    b2[9:00\ncreated] --> b3
    b3[10:00\ncreated] --> b4:::disabled
    b4[11:00\nskipped] --> b5:::disabled
    end

    subgraph g3[Players online]
    b5[12:00\nskipped] --> b6
    b6[13:00\ncreated]
    end
    
    classDef disabled fill:#eee,stroke:#aaa,color:gray
```

!!! note

    This feature requires the Prime Backup plugin to be loaded **before the server starts**.
    It's needed so Prime Backup can calculate the online player count correctly

    If the Prime Backup plugin is loaded when the server is already running, 
    the player detection feature will be disabled, as if [require_online_players](#require_online_players) is set to `false`

    Exception: If [MinecraftDataAPI](https://github.com/MCDReforged/MinecraftDataAPI) plugin is present,
    then Prime Backup will utilize its API to query online player dynamically, and the requirement mentioned above no longer exists

!!! note

    The player detection logic might not work in heavily modded servers that behave too differently from vanilla servers

!!! tip

    Since Prime Backup supports file deduplication and highly customizable [prune config](#prune-config),
    having backups even when no player is on the server will not result in excessive space usage

- Type: `bool`
- Default: `false`

#### require_online_players_blacklist

In [require_online_players](#require_online_players), when determining if there are players online, 
the list of regex patterns for player names to be excluded

If you want the server to still be considered as having no players online and thus not perform scheduled backups,
when only certain players are online, you can utilize this option

Example:

```json
"require_online_players_blacklist": [
    "bot_.*",  // Matches any player name starting with "bot_"
    "Steve"    // Matches the player name "Steve"
]
```

!!! note

    The regex patterns will perform [full match](https://docs.python.org/3/library/re.html#re.fullmatch) checks on the player names
   
    Matches are **case-insensitive**

- Type: `List[re.Pattern]`
- Default: `[]`

---

### Prune config

The backup prune feature from Prime Backup enables automatically backup cleanup for the backup storage

```json
{
    "enabled": true,
    "interval": "3h",
    "crontab": null,
    "jitter": "20s",
    "timezone_override": null,
    "regular_backup": {
        "enabled": false,
        "max_amount": 0,
        "max_lifetime": "0s",
        "last": -1,
        "hour": 0,
        "day": 0,
        "week": 0,
        "month": 0,
        "year": 0
    },
    "temprory_backup": {
        "enabled": true,
        "max_amount": 10,
        "max_lifetime": "30d",
        "last": -1,
        "hour": 0,
        "day": 0,
        "week": 0,
        "month": 0,
        "year": 0
    }
}
```

It contains 2 prune settings for 2 kinds of backups:

- `regular_backup`: For regular backups, i.e. not temporary backups
- `temporary_backup`: For temporary backups, e.g. pre-restore backups

Each prune settings describes the retain policy in detailed

Prime Backup use the following steps to decide what to delete and what to retain:

1. Use [`last`, `hour`, `day`, `week`, `month`, `year`](#last-hour-day-week-month-year),
   based on the [PBS](https://pbs.proxmox.com/docs/prune-simulator/) policy, filter out backups to be deleted / retained
2. Use `max_amount` and `max_lifetime` to filter out those old and expired backups from the to-be-retained backups from step 1
3. Collected those to-be-deleted backups from the 2 steps above, delete them one by one

The final prune result will be stored in the log file at `logs/prune.log` within the [storage root](#storage_root)

``` title="prune.log"
Backup #147 at 2023-12-10 21:49:09: keep=True reason=keep last 1
Backup #146 at 2023-12-10 21:36:10: keep=True reason=keep last 2
Backup #145 at 2023-12-10 21:26:25: keep=True reason=keep last 3
Backup #144 at 2023-12-10 21:21:22: keep=False reason=superseded by 145 (hour)
Backup #143 at 2023-12-10 21:16:19: keep=False reason=superseded by 145 (hour)
Backup #142 at 2023-12-10 21:11:14: keep=False reason=superseded by 145 (hour)
Backup #141 at 2023-12-10 21:05:06: keep=False reason=superseded by 145 (hour)
Backup #140 at 2023-12-10 21:00:03: keep=True reason=protected
Backup #139 at 2023-12-10 20:55:01: keep=True reason=keep hour 1
Backup #138 at 2023-12-10 20:49:57: keep=False reason=superseded by 139 (hour)
Backup #137 at 2023-12-10 20:44:53: keep=False reason=superseded by 139 (hour)
Backup #136 at 2023-12-10 20:39:45: keep=False reason=superseded by 139 (hour)
Backup #135 at 2023-12-10 20:34:41: keep=False reason=superseded by 139 (hour)
Backup #128 at 2023-12-10 19:59:06: keep=True reason=keep hour 2
Backup #116 at 2023-12-10 18:56:14: keep=True reason=keep hour 3
Backup #104 at 2023-12-10 17:55:35: keep=False reason=superseded by 116 (day)
Backup #22 at 2023-12-09 23:59:53: keep=True reason=keep day 1
```

#### max_amount

Defines the maximum number of backups to keep, e.g. `10` means to keep a maximum of 10 of the latest backups

Set it to `0` for unlimited

- Type: `int`

#### max_lifetime

Defines the maximum retention duration for all backups. Backups exceeding this duration will be pruned and deleted.

Set it to `0s` for no time limit

- Type: [`Duration`](#duration)

#### last, hour, day, week, month, year

A set of [PBS](https://pbs.proxmox.com/)-style prune options, to describe how backups are deleted/retained

See the [prune simulator](https://pbs.proxmox.com/docs/prune-simulator/) for more explanations on these options

The [prune simulator](https://pbs.proxmox.com/docs/prune-simulator/) can also be used to simulate the retain policy

Notes: Value `0` means no retention on this period; Value `-1` means unlimited backups can be retained on this period, equivalent to setting to a very large value

- Type: `int`

---

#### enabled, interval, crontab, jitter

See the [crontab job setting](#crontab-job-setting) section

#### timezone_override

An optional timezone override during the prune calculation. By default (`null` value), Prime Backup will use the local timezone

Example values: `null`, `"Asia/Shanghai"`, `"US/Eastern"`, `"Europe/Amsterdam"`

- Type: `Optional[str]`
- Default: `null`

---

### Database config

Configurations for the SQLite database, used by Prime Backup

```json
{
    "compact": {
        "enabled": true,
        "interval": null,
        "crontab": "0 7 * * 0",
        "jitter": "1m"
    },
    "backup": {
        "enabled": true,
        "interval": null,
        "crontab": "0 6 * * 0",
        "jitter": "1m"
    }
}
```

Subconfig `compact` and `backup` describe the crontab jobs on the database

#### compact

The database compact job

It performs the [VACUUM](https://www.sqlite.org/lang_vacuum.html) command on the database, to compact the database file and free up unused space

#### backup

The database backup job

By default, Prime Backup creates backups for the database in the `db_backup` directory
within the [storage root](#storage_root) periodically, just in case something wrong happens

Database backups are stored with the `.tar.xz` format, and won't take up much space

#### enabled, interval, crontab, jitter

See the [crontab job setting](#crontab-job-setting) section

---

## Subconfig types

### crontab job setting

Setting of a crontab job to describe when the job executes. There are 2 valid mode

- interval mode: Execute the job at given time interval. The first execution also has to wait for the given interval
- crontab mode: Execute the job at specific time, described by a crontab string

If the job is enabled, you must choose one of the above modes and set the related config values correctly

```json
// Example
{
    "enabled": true,
    "interval": "1h",
    "crontab": null,
    "jitter": "10s"
}
```

#### enabled

The switch for the job. Set to  `true` to enable the crontab job; Set to `false` to disable the crontab job

- Type: `bool`

#### interval

Used in interval mode. The time interval between 2 jobs

It should be `null` if the job is not in interval mode

- Type: `Optional[str]`

#### crontab

Used in crontab mode. The crontab string

You can use [https://crontab.guru/](https://crontab.guru/) to create a crontab string

It should be `null` if the job is not in crontab mode

- Type: `Optional[str]`

#### jitter

The jitter between 2 scheduled backup jobs

The actual execution time for the next job will be shifted randomly with `[0, jitter]`

Set it to `"0s"` for no jitter

- Type: `str`

---

## Special value types

### Duration

Describes a time duration length with a string, e.g. `"3s"`, `"15m"`

A Duration consists of two parts: the number, and the time unit.

For the number part, it can be an integer, or a float

For the unit part, see the following table:

| Unit           | Description | Equals to    | Value in seconds |
|----------------|-------------|--------------|------------------|
| `ms`           | millisecond | 0.001 second | 0.001            |
| `s`, `sec`     | second      | 1 second     | 1                |
| `m`, `min`     | minute      | 60 seconds   | 60               |
| `h`, `hour`    | hour        | 60 minutes   | 3600             |
| `d`, `day`     | day         | 24 hours     | 86400            |
| `mon`, `month` | month       | 30 days      | 2592000          |
| `y`, `year`    | year        | 365 days     | 31536000         |
