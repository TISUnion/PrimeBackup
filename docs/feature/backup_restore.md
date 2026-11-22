---
title: 'Backup Restoration'
---

Restore a backup

## Restoration Steps

### MCDR Environment

To restore a backup in the MCDR environment, run:

```
!!pb back
```

This restores to the latest non-temporary backup. To restore to a specific backup, pass its backup ID:

```
!!pb back 78
```

Example console output:

```
> !!pb back 78
[MCDR] [22:08:24] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] Gonna restore the world to backup #78: Test Backup
[MCDR] [22:08:24] [PB@f133-worker-heavy/INFO]: [PB] Please choose and enter command within 1 minute:
[MCDR] [22:08:24] [PB@f133-worker-heavy/INFO]: [PB] - Confirm restore√: !!pb confirm
[MCDR] [22:08:24] [PB@f133-worker-heavy/INFO]: [PB] - Abort restore×: !!pb abort
> !!pb confirm
[MCDR] [22:08:26] [TaskExecutor/INFO]: [PB] Confirming restore backup task
[MCDR] [22:08:26] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 10 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:27] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 9 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:28] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 8 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:29] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 7 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:30] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 6 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:31] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 5 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:32] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 4 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:33] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 3 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:34] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 2 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:35] [PB@f133-worker-heavy/INFO] [prime_backup]: [PB] !!! 1 second later, the world will be restored to backup #78: Test Backup
[MCDR] [22:08:36] [PB@f133-worker-heavy/INFO] [prime_backup]: Wait for server to stop
[Server] [22:08:36] [Server thread/INFO]: Stopping the server
[Server] [22:08:36] [Server thread/INFO]: Stopping server
[Server] [22:08:36] [Server thread/INFO]: Saving players
[Server] [22:08:36] [Server thread/INFO]: Saving worlds
[Server] [22:08:36] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_nether
[Server] [22:08:36] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:36] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_end
[Server] [22:08:36] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[Server] [22:08:36] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:overworld
[Server] [22:08:37] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[Server] [22:08:37] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:37] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[Server] [22:08:37] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[MCDR] [22:08:37] [MainThread/INFO]: Server process stopped with code 0
[MCDR] [22:08:37] [MainThread/INFO]: Server stopped
[MCDR] [22:08:37] [PB@f133-worker-heavy/INFO] [prime_backup]: Creating backup of existing files to avoid idiot
[MCDR] [22:08:37] [PB@f133-worker-heavy/INFO] [prime_backup]: Scanning file for backup creation at path 'server', targets: ['world']
[MCDR] [22:08:38] [PB@f133-worker-heavy/INFO] [prime_backup]: Creating backup for ['world'] at path 'server', file cnt 4118, timestamp 1765128818483891, creator 'prime_backup:pre_restore', comment '__pb_translated__:pre_restore:78', tags {'temporary': True}
[MCDR] [22:08:39] [PB@f133-worker-heavy/INFO] [prime_backup]: Create backup #83 done, +5 blobs (size 6.42MiB / 7.27MiB)
[MCDR] [22:08:39] [PB@f133-worker-heavy/INFO] [prime_backup]: Restoring to backup #78 (fail_soft=False, verify_blob=True)
[MCDR] [22:08:39] [PB@f133-worker-heavy/INFO] [prime_backup]: Exporting Backup(id=78, timestamp=1763890381206484, creator='player:Fallen_Breath', comment='Test Backup', targets=['world'], tags={}, fileset_id_base=56, fileset_id_delta=88, file_count=4118, file_raw_size_sum=136177532, file_stored_size_sum=78537902) to directory server
[MCDR] [22:08:48] [PB@f133-worker-heavy/INFO] [prime_backup]: Export done
[MCDR] [22:08:48] [PB@f133-worker-heavy/INFO] [prime_backup]: Restore to backup #78 done, cost 10.3s (backup 1.79s, restore 8.51s), starting the server
[MCDR] [22:08:48] [PB@f133-worker-heavy/INFO]: Starting the server with command 'java -Xms1G -Xmx2G -jar server.jar'
```

Example in-game output:

![pb back](img/pb_back.png)

The restore command accepts multiple backup ID formats:

| Format           | Example        | Description                                        |
|------------------|----------------|----------------------------------------------------|
| Positive integer | `!!pb back 12` | Restore to the backup with the specified ID        |
| `~` or `latest`  | `!!pb back ~`  | Restore to the latest non-temporary backup         |
| Relative offset  | `!!pb back ~1` | Restore to 1 backup before the latest              |
| Relative offset  | `!!pb back ~3` | Restore to 3 backups before the latest             |

Optional parameters:

| Parameter     | Description                                       |
|---------------|---------------------------------------------------|
| `--confirm`   | Skip confirmation and start restoring immediately |
| `--fail-soft` | Skip files that fail to export during restoration |
| `--no-verify` | Skip content verification for exported files      |

Example:

```
!!pb back 12 --confirm --fail-soft
```

### Command Line Environment

Prime Backup also supports core restoration from the command line:

```
python3 PrimeBackup.pyz -d pb_files -c config/prime_backup/config.json back -s ./server 78 --confirm
```

The CLI `back` command does not stop or start the Minecraft server; stop the server yourself before running it.
If `restore.create_pre_restore_backup` is enabled, a temporary backup of the existing contents in `-s/--source-root` is created before restoring. If that directory does not exist, the pre-restore backup is skipped.
Pass `--no-pre-restore-backup` to skip this temporary backup

## Related Configuration

Restoration options are mainly located in the following three sections:

- [Server config](../config.md#server-config): commands used to interact with the Minecraft server
- [Backup config](../config.md#backup-config): file filtering and handling rules during restoration
- [Restore config](../config.md#restore-config): restore destination directory and strategy options

## Detailed Restoration Process

1. Confirmation phase
   1. Display information about the target backup
   2. Wait for user confirmation (unless `--confirm` is passed)
   3. Confirm with `!!pb confirm` or cancel with `!!pb abort`
2. Server shutdown
   1. If the server is running, run a countdown (default `restore.countdown_sec: 10`)
   2. Restoration can still be aborted during the countdown
   3. Stop the server and wait until it exits completely
3. Pre-restore backup
   1. If `restore.create_pre_restore_backup` is enabled (default `true`) and the restore destination exists, create a temporary backup of its current contents. The source is `restore.destination_root`, or `backup.source_root` when `destination_root` is `null`
   2. The comment is `"Automatic backup before restoring to #X"`
   3. This backup is tagged as temporary and handled separately during backup pruning
4. File restoration
   1. Recycle bin: move existing files in the target directory to a temporary recycle bin so everything can be rolled back if restoration fails
   2. Retained files: if `retain_patterns` is configured, match and set aside files that should remain untouched
   3. File export: export backup files to the target directory in parallel; if `restore.reuse_unchanged_files` is enabled, unchanged files are moved back from the recycle bin instead
   4. Metadata restoration: restore permissions, timestamps, ownership, and symbolic link targets
   5. Retained file restoration: move retained files back to their original paths
5. Server restart
   1. Restart the server if it was running before the restore
