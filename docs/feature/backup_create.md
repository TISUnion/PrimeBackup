---
title: 'Backup Creation'
---

Creating a Backup

## Backup Steps

### MCDR Environment

To create a backup in the MCDR environment, simply execute the following command:

```
!!pb make
```

That's it, just one simple command to create a backup

Example console output:

```
> !!pb make
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Creating backup, please wait
[Server] [00:46:11] [Server thread/INFO]: Automatic saving is now disabled
[Server] [00:46:11] [Server thread/INFO]: Saving the game (this may take a moment!)
[Server] [00:46:11] [Server thread/INFO]: ThreadedAnvilChunkStorage (world15): All chunks are saved
[Server] [00:46:11] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [00:46:11] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[Server] [00:46:11] [Server thread/INFO]: Saved the game
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: Scanning file for backup creation at path 'server', targets: ['world']
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: Creating backup for ['world'] at path 'server', file cnt 4106, timestamp 1760892371872862, creator 'console:', comment '', tags {}
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: Create backup #59 done, +10 blobs (size 11.82MiB / 16.75MiB)
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: Time costs: save wait 0.1s, create backup 1.49s
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Backup completed, ID #59, time cost 1.6s, total 118.39MiB (+16.75MiB)
[Server] [00:46:12] [Server thread/INFO]: Automatic saving is now enabled
```

Example in-game output:

![pb make](img/pb_make.png)

To attach a comment for easier identification later, run:

```
!!pb make <comment>
```

For example:

```
!!pb make Example Comment Here
```

Once created, you can view the backup with `!!pb show` or `!!pb list`. See [Backup Display](backup_display.md) for details

![pb make and show](img/pb_make_show.png)

### Command Line Environment

Prime Backup also supports core backup creation from the command line:

```
python3 PrimeBackup.pyz -d pb_files -c config/prime_backup/config.json make -s ./server "Example Comment Here"
```

The CLI `make` command does not interact with the Minecraft server — it will not disable autosave, issue save commands, or wait for world-save completion. Save or stop the server yourself before running it


## Related Configuration

Backup creation options are mainly located in the following two sections:

- [Server config](../config.md#server-config): how Prime Backup interacts with the Minecraft server during backup creation
- [Backup config](../config.md#backup-config): core options for creating backups

Commonly used options:

| Option                                                                       | Description                                                                                    |
|------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| [server.turn_off_auto_save](../config.md#turn_off_auto_save)                 | Whether to temporarily disable Minecraft autosave during backup creation                       |
| [backup.targets](../config.md#targets)                                       | Files and directories to back up                                                               |
| [backup.ignore_patterns](../config.md#ignore_patterns)                       | Patterns for files to exclude during backup and delete during restore                          |
| [backup.retain_patterns](../config.md#retain_patterns)                       | Patterns for files to exclude during backup while keeping them untouched during restore        |
| [backup.creation_skip_missing_file](../config.md#creation_skip_missing_file) | Skip files that disappear while a backup is being created                                      |
| [backup.hash_method](../config.md#hash_method)                               | Hash algorithm used to deduplicate files with identical content                                |
| [backup.compress_method](../config.md#compress_method)                       | Compression method used when storing backup data                                               |
| [concurrency](../config.md#concurrency)                                      | Maximum worker concurrency; raising it appropriately can speed up hashing on multi-core hosts  |


!!! note

    It is best to configure `hash_method` and `compress_method` before creating your first backup. If backups already exist:

    - `hash_method` cannot be changed directly in the config file, or Prime Backup will fail to load. Use `!!pb database migrate_hash_method <algorithm>` to migrate
    - `compress_method` can be changed directly, but it only applies to newly stored data. To recompress existing data, run `!!pb database migrate_compress_method <method>`


## Detailed Backup Process

1. Prepare the Minecraft server (all steps below are configurable)
    1. Disable autosave (e.g. `save-off`)
    2. Flush world data to disk (e.g. `save-all flush`)
    3. Wait for the server save confirmation log (e.g. `Saved the game`)
2. Create the backup
    1. Scan backup targets and filter out ignored files to build the file list with metadata
    2. If `concurrency > 1`, pre-compute file hashes in parallel with multiple threads
    3. For each file:
        1. Compute its hash, reusing the pre-computed value if available
        2. Check whether the content is already stored; if not, store it as a direct blob or as deduplicated chunks in pack files when chunking applies
        3. Record the file metadata and hash in the database
    4. Allocate and save the fileset for this backup
3. Restore Minecraft server state
    1. Re-enable autosave if it was disabled in step 1
