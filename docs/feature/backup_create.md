---
title: 'Backup Creation'
---

Creating a Backup

## Backup Steps

### MCDR Environment

Prime Backup currently only supports backup creation in MCDR environment

To create a backup in MCDR environment, simply execute the following command:

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

If you want to add a comment to the backup for easier identification later, you can use the following command

```
!!pb make <comment>
```

For example

```
!!pb make Roasted pig is ready
```

After the backup is created, you can use `!!pb show` or `!!pb list` commands to view it

For specific operation steps, please refer to the [Backup Display](backup_display.md) documentation

![pb make and show](img/pb_make_show.png)

### Command Line Environment

Prime Backup currently does not support creating backups in command line environment (maybe next time!)


## Related Configuration

Configuration related to creation is mainly located in the following two sections:

- [Server Configuration](../config.md#server-configuration), including how PB interacts with MC server during backup creation process
- [Backup Configuration](../config.md#backup-configuration), including various options related to backup creation

Below are some commonly used configuration items

| Option                                                                       | Function                                                                                                          |
|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| [server.turn_off_auto_save](../config.md#turn_off_auto_save)                 | Whether to temporarily disable MC's automatic saving during backup creation                                       |
| [backup.targets](../config.md#targets)                                       | Modify backup targets, adjust which files/folders to backup                                                       |
| [backup.ignore_patterns](../config.md#ignore_patterns)                       | Ignore certain files during backup, and delete these files during restore                                         |
| [backup.retain_patterns](../config.md#retain_patterns)                       | Ignore certain files during backup, but keep these files unchanged during restore                                 |
| [backup.creation_skip_missing_file](../config.md#creation_skip_missing_file) | Skip files that suddenly don't exist during backup                                                                |
| [backup.hash_method](../config.md#hash_method)                               | Hash algorithm. PB will reuse files with the same hash value to reduce storage space usage                        |
| [backup.compress_method](../config.md#compress_method)                       | Compression algorithm. The compression algorithm used by PB when storing files                                    |
| [concurrency](../config.md#concurrency)                                      | Maximum concurrency. Appropriately increase this value according to system resources to speed up hash calculation |


!!! note

    For the two configuration items `hash_method` and `compress_method`, it is recommended to configure them before creating the first backup. For environments that have already created backups, if you need to modify the above two configuration items, please note:

    - The `hash_method` configuration item cannot be directly modified. If directly modified, it will cause PB to fail to load. You need to use the `!!pb database migrate_hash_method <xxx>` command to migrate
    - The `compress_method` configuration item can be directly modified, but the modification only affects newly added files. If you need to modify the compression algorithm of existing data, you can use the `!!pb database migrate_compress_method <xxx>` command


## Detailed Backup Process

Below will list the operation process during PB backup creation

1. Interact with MC server to construct an environment suitable for backup creation (all operations below can be configured)
    1. Use commands such as `save-off` to disable MC's automatic saving
    2. Use commands such as `save-all flush` to make MC save game data to disk
    3. Wait for MC to output logs such as `Saved the game`, at this point the game save is complete and backup creation can begin
2. Create backup
    1. Scan the backup targets, exclude ignored files, and finally get a list of files to backup, along with file metadata
    2. If available concurrency configuration > 1, use multi-threading to pre-calculate the hash value of each file to be backed up
    3. For each file to be backed up, process one by one:
        1. Calculate hash value. If previously calculated, reuse directly
        2. Determine if data with this hash value has been backed up before, if not, copy a copy of the data to the backup path
        3. Store this file's metadata and hash value in the database
    4. Calculate the fileset for this backup, complete the backup
3. Restore operations on MC server
    1. If MC's automatic saving was disabled, it will be re-enabled here
