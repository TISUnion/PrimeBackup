---
title: 'Backup Restoration (Rollback)'
---

Restoring a backup, also known as rollback operation

## Restoration Steps

### MCDR Environment

Prime Backup currently only supports backup restoration in MCDR environment

To restore a backup in MCDR environment, simply execute the following command:

```
!!pb back
```

This command will restore to the latest non-temporary backup. To restore to a specific backup, you can specify the backup ID:

```
!!pb back 78
```

Example console output:

```
> !!pb back 78
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] Will restore to backup#78: Test backup
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] Please make a choice within 1 minute and enter the corresponding command:
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] - Confirm restore: !!pb confirm
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] - Abort restore×: !!pb abort
> !!pb confirm
[MCDR] [22:08:08] [TaskExecutor/INFO]: [PB] Confirming restore task
[MCDR] [22:08:08] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 10 seconds
[MCDR] [22:08:09] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 9 seconds
[MCDR] [22:08:10] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 8 seconds
[MCDR] [22:08:11] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 7 seconds
[MCDR] [22:08:12] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 6 seconds
[MCDR] [22:08:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 5 seconds
[MCDR] [22:08:14] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 4 seconds
[MCDR] [22:08:15] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 3 seconds
[MCDR] [22:08:16] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 2 seconds
[MCDR] [22:08:17] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! Will restore to backup#78: Test backup in 1 second
[MCDR] [22:08:18] [PB@ecac-worker-heavy/INFO] [prime_backup]: Wait for server to stop
[Server] [22:08:18] [Server thread/INFO]: Stopping the server
[Server] [22:08:18] [Server thread/INFO]: Stopping server
[Server] [22:08:18] [Server thread/INFO]: Saving players
[Server] [22:08:18] [Server thread/INFO]: Saving worlds
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:overworld
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_nether
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_end
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[MCDR] [22:08:19] [MainThread/INFO]: Server process has stopped, return code 0
[MCDR] [22:08:19] [MainThread/INFO]: Server has been shut down
[MCDR] [22:08:19] [PB@ecac-worker-heavy/INFO] [prime_backup]: Creating backup of existing files to avoid idiot
[MCDR] [22:08:19] [PB@ecac-worker-heavy/INFO] [prime_backup]: Scanning file for backup creation at path 'server', targets: ['world']
[MCDR] [22:08:20] [PB@ecac-worker-heavy/INFO] [prime_backup]: Creating backup for ['world'] at path 'server', file cnt 4118, timestamp 1764526100048797, creator 'prime_backup:pre_restore', comment '__pb_translated__:pre_restore:78', tags {'temporary': True}
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Create backup #79 done, +6 blobs (size 6.43MiB / 7.28MiB)
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restoring to backup #78 (fail_soft=False, verify_blob=True)
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Exporting Backup(id=78, timestamp=1763890381206484, creator='player:Fallen_Breath', comment='Test backup', targets=['world'], tags={}, fileset_id_base=56, fileset_id_delta=88, file_count=4118, file_raw_size_sum=136177532, file_stored_size_sum=78537902) to directory server
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO] [prime_backup]: Export done
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restore to backup #78 done, cost 16.95s (backup 2.67s, restore 14.28s), starting the server
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO]: Starting the server, startup parameters 'java -Xms1G -Xmx2G -jar server.jar'
```

Example in-game output:

![pb back](img/pb_back.png)

The restore command supports multiple backup ID formats:

| Format             | Example             | Description            |
|----------------|----------------|---------------|
| Positive integer            | `!!pb back 12` | Restore to backup with specified ID  |
| `~` or `latest` | `!!pb back ~`  | Restore to latest non-temporary backup   |
| Relative offset           | `!!pb back ~1` | Restore to the backup before the latest backup |
| Relative offset           | `!!pb back ~3` | Restore to the backup three before the latest backup |

The restore command supports the following optional parameters:

| Parameter            | Description              |
|---------------|-----------------|
| `--confirm`   | Skip confirmation step, directly start restoration   |
| `--fail-soft` | Skip files that fail to export during export process |
| `--no-verify` | Do not verify the content of exported files      |

Example:

```
!!pb back 12 --confirm --fail-soft
```

### Command Line Environment

Prime Backup currently does not support restoring backups in command line environment (maybe next time!)

## Related Configuration

Configuration related to restoration is mainly located in the following two sections:

- [Server Configuration](../config.md#server-configuration), including interaction commands with MC server during restoration
- [Backup Configuration](../config.md#backup-configuration), including file processing rules during restoration

## Detailed Restoration Process

Below will list the operation process during PB backup restoration

1. Confirmation phase
   1. Display the backup information to be restored
   2. Wait for user confirmation (unless using `--confirm` parameter)
   3. User can confirm with `!!pb confirm` or abort with `!!pb abort`
2. Server shutdown
   1. If the server is running, execute 10s countdown (default configuration `command.restore_countdown_sec: 10`)
   2. Restoration can be canceled during countdown
   3. Stop the server and wait for complete shutdown
3. Pre-restoration backup
   1. If `backup_on_restore` is configured (default value `true`), create a backup at this time for emergency use
   2. Backup comment is "Automatic backup before restoring to #X"
   3. This backup will be marked as temporary and will be specially handled during backup cleanup
4. Actual restoration operation
   1. Recycle bin mechanism: Move all existing files in the backup target directory to a temporary recycle bin, ensuring complete rollback if restoration fails
   2. Retain file processing: If `retain_patterns` is configured, use gitignore-style pattern matching and isolate files to be retained
   3. File export: Use multi-threading to export backup files to the target directory in parallel
   4. Attribute restoration: Restore file permissions, timestamps, owners, and symbolic link targets
   5. Retain file restoration: Finally move `retain_patterns` retained files back to their original positions
5. Server restart
   1. If the server was originally running, restart the server