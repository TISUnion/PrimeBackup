# Miscellaneous Tools

## [qb_importer.py](qb_importer.py)

Import backups from [QuickBackupM](https://github.com/TISUnion/QuickBackupM) to Prime Backup

```bash
$ python3 qb_importer.py --help
usage: qb_importer.py [-h] -i INPUT -x EXECUTABLE -d DB [-t TEMP] [-c CREATOR] [-s SLOT]

A tool to import backups from QuickBackupMulti to PrimeBackup

Example usages:

  qb_importer.py --help
  qb_importer.py -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files -c Steve
  qb_importer.py -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files --slot 1

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Path to the QuickBackupMulti backup file root, e.g. /path/to/qb_multi 
                        (default: None)
  -x EXECUTABLE, --executable EXECUTABLE
                        Path to the PrimeBackup plugin file (default: None)
  -d DB, --db DB        Path to the PrimeBackup file root that contains the database file and so
                        on, e.g. /path/to/pb_files (default: None)
  -t TEMP, --temp TEMP  Path for placing temp files for import (default: ./qb_importer_temp)
  -c CREATOR, --creator CREATOR
                        Creator of the imported backup (default: QuickBackupM)
  -s SLOT, --slot SLOT  Specified the slot number to import. If not provided, import all slots
                        (default: None)
```

## [issue_64_fixer.py](issue_64_fixer.py)

A data-recover helper tool for [Issue #64](https://github.com/TISUnion/PrimeBackup/issues/64)

> [!NOTE]  
> You need to have external backups of these 2 stuffs perform the fix:
> 1. Backup of the database file (created by PB v1.9+). Take a look in `pb_files/db_backup`
> 2. (Optional) Backup of the blob store (`pb_files/blobs`) backups
> 
> No rescue if no backup :(

```bash
$ python3 ./issue_64_fixer.py --help
usage: issue_64_fixer.py [-h] --pb-plugin PB_PLUGIN --pb-dir PB_DIR
                         [--db-backup-dir DB_BACKUP_DIR] [--blobs-backup-dir BLOBS_BACKUP_DIR]

optional arguments:
  -h, --help            show this help message and exit
  --pb-plugin PB_PLUGIN
                        Path to the Prime Backup plugin file (default: None)
  --pb-dir PB_DIR       Path to the pb_files directory (default: None)
  --db-backup-dir DB_BACKUP_DIR
                        Path to the directory that contains database backup files. All .db files    
                        inside will be used. You can ignore this argument if you dont need to       
                        recover database file objects (default: None)
  --blobs-backup-dir BLOBS_BACKUP_DIR
                        Path to the directory that contains blobs directory backups. Directories    
                        inside should be something like blobs1, blobs2, blobs3, where each
                        directory is a valid blobs directory from pb_files/blobs. You can ignore    
                        this argument if you dont need to recover blobs (default: None)
```

### Prepare

> [!IMPORTANT]
> This tool assert that no hash method / compress method migration was done after those external data backups are made

First, confirm the time you upgraded your Prime Backup plugin to affected version (v1.9.0 ~ v1.9.2). Let's say you upgraded PrimeBackup at time T0

Second, you need to collect external backups of these 2 things since T0:

1. Prime Backup database file, i.e. `prime_backup.db`. By default, Prime Backup enables a scheduled database backup feature, 
   and creates a few database backups inside `/path/to/pb_files/db_backup`. You can try using these database backup files
2. (Optional, you can first try without) Prime Backup blob store backups. i.e. backups of `pb_files/blobs`.
   External backups / VM backups / Disk backup, try your best to collect them 

Then categorize them into folders for storage, such as:

```
db_backups/
+-- prime_backup_20250317.db
+-- prime_backup_20250324.db
+-- ...

blobs_backups/
+-- blobs_20250317
    +-- 00
        +-- 000a9e1e9110bbbabe023114f4eb1c59029aa255682b5f1100fd454a5ace95c5
    +-- 01
    +-- ...
+-- blobs_20250324
+-- ...
```

### Run it

> [!TIP]
> Before run anything, back everything up (the `pb_files` directory), in case of something bad happens

First, you need to locate the Prime Backup instance you want to fix (e.g. path to the `pb_files` directory), and prepare a valid Prime Backup plugin file

> [!NOTE]  
> The tool is tested with Prime Backup v1.9.4. Newer versions might still work

Then, enter a python environment with all Prime Backup's required python packages installed.
Now you can run the fix tool by:

```bash
python3 ./issue_64_fixer.py --pb-plugin /path/to/PrimeBackup-v1.9.4.pyz --pb-dir /path/to/pb_files --db-backup-dir ./db_backups --blobs-backup-dir ./blobs_backups
# or multiline
python3 ./issue_64_fixer.py \
  --pb-plugin /path/to/PrimeBackup-v1.9.4.pyz \
  --pb-dir /path/to/pb_files \
  --db-backup-dir ./db_backups \
  --blobs-backup-dir ./blobs_backups
```

You can omit `--blobs-backup-dir ./blobs_backups` if you don't have any blob store backups yet.
In some lucky cases, there might be 0 affected blob and no blob restore is required

The script is re-enter-able. Feel free to re-run it if you collect more external backups

Example script output with comments:

```bash
# This '/path/to/pb_files' should be the Prime Backup instance you want to fix, value from --pb-dir
[2025-03-30 01:49:05,402 INFO] (__init_pb_environment) Storage root set to '/path/to/pb_files'

# First, the script will perform a fileset validation to figure out those corrupt filesets
[2025-03-30 01:49:05,423 INFO] (__get_bad_base_fileset_ids) Locating bad fileset ids
[2025-03-30 01:49:05,423 INFO] (run) Fileset validation start
[2025-03-30 01:49:05,424 INFO] (run) Validating 492 fileset objects
[2025-03-30 01:49:05,426 INFO] (run) Validating 200 / 492 fileset objects
[2025-03-30 01:49:06,747 INFO] (run) Validating 400 / 492 fileset objects
[2025-03-30 01:49:07,953 INFO] (run) Validating 492 / 492 fileset objects
[2025-03-30 01:49:08,228 INFO] (run) Fileset validation done: total 492, validated 492, ok 434, bad 58

# Here's the file validation result
# We only need to fix the base filesets
[2025-03-30 01:49:08,228 INFO] (__get_bad_base_fileset_ids) Found 58 bad filesets
[2025-03-30 01:49:08,228 INFO] (__get_bad_base_fileset_ids) Found 2 bad base filesets
[2025-03-30 01:49:08,228 INFO] (__get_bad_base_fileset_ids) 399, 451

# Then, the script will try its best to recover file objects for these bad filesets, from all database files inside --db-backup-dir
[2025-03-30 01:49:08,279 INFO] (__read_backup_db) Read 11075 file rows for fileset 399 from database '/path/to/db_backups/prime_backup_20250317.db'
[2025-03-30 01:49:08,320 INFO] (__read_backup_db) Read 11217 file rows for fileset 451 from database '/path/to/db_backups/prime_backup_20250317.db'
[2025-03-30 01:49:08,324 INFO] (__read_backup_db) Read 265 file rows for fileset 399 from database '/path/to/db_backups/prime_backup_20250324.db'
[2025-03-30 01:49:08,364 INFO] (__read_backup_db) Read 11217 file rows for fileset 451 from database '/path/to/db_backups/prime_backup_20250324.db'
[2025-03-30 01:49:08,367 INFO] (run) Recovering files for base fileset 399
[2025-03-30 01:49:08,583 INFO] (run) Recovering 10810 files for fileset 399
[2025-03-30 01:49:08,584 INFO] (run) Recovering files for base fileset 451
[2025-03-30 01:49:09,510 INFO] (run) Recovering 11154 files for fileset 451
[2025-03-30 01:49:09,861 INFO] (run) Recovered 21964 files in total

# File object recover done. Perform another validation to double check the state
[2025-03-30 01:49:09,861 INFO] (run) Perform another filesets validation since new files were added
[2025-03-30 01:49:09,861 INFO] (run) Fileset validation start
[2025-03-30 01:49:09,862 INFO] (run) Validating 492 fileset objects
[2025-03-30 01:49:09,862 INFO] (run) Validating 200 / 492 fileset objects
[2025-03-30 01:49:11,215 INFO] (run) Validating 400 / 492 fileset objects
[2025-03-30 01:49:12,408 INFO] (run) Validating 492 / 492 fileset objects
[2025-03-30 01:49:13,079 INFO] (run) Fileset validation done: total 492, validated 492, ok 492, bad 0
[2025-03-30 01:49:13,080 INFO] (run) NICE, fileset fix done

# Rebuild those missing blob object in the database
[2025-03-30 01:49:13,269 INFO] (run) Iterating all file objects in the database to check if there is any missing blob object
[2025-03-30 01:49:13,949 INFO] (run) Recovered 44 database blob in total

# Now it's the time to recover those deleted blob files in the blob store
# if --blobs-backup-dir is not provided, this step will be skipped
[2025-03-30 01:49:14,125 INFO] (__find_blob_store_roots) Searching at 'blobs_backups' for blob storage roots, for hash_method blake3 with hex length 64
[2025-03-30 01:49:14,125 INFO] (__find_blob_store_roots) Found blob store root at '/path/to/blobs_backups/blobs_20250324'
[2025-03-30 01:49:14,125 INFO] (__find_blob_store_roots) Found 1 blob store roots in total
[2025-03-30 01:49:14,126 INFO] (run) Iterating all blobs in database to check if there is any missing blobs
[2025-03-30 01:49:14,644 INFO] (run) Found 9 blobs with their file missing, and recovered 9 blob files in total

# Blob file recover done, perform a blob validation as a double check
[2025-03-30 01:49:14,653 INFO] (run) Perform a blob validation since new files were added
[2025-03-30 01:49:14,653 INFO] (run) Blob validation start
[2025-03-30 01:49:14,678 INFO] (run) Validating 3000 / 31765 blobs
[2025-03-30 01:49:15,643 INFO] (run) Validating 6000 / 31765 blobs
[2025-03-30 01:49:17,689 INFO] (run) Validating 9000 / 31765 blobs
[2025-03-30 01:49:18,995 INFO] (run) Validating 12000 / 31765 blobs
[2025-03-30 01:49:30,261 INFO] (run) Validating 15000 / 31765 blobs
[2025-03-30 01:49:41,155 INFO] (run) Validating 18000 / 31765 blobs
[2025-03-30 01:49:52,696 INFO] (run) Validating 21000 / 31765 blobs
[2025-03-30 01:50:03,684 INFO] (run) Validating 24000 / 31765 blobs
[2025-03-30 01:50:13,825 INFO] (run) Validating 27000 / 31765 blobs
[2025-03-30 01:50:24,497 INFO] (run) Validating 30000 / 31765 blobs
[2025-03-30 01:50:35,301 INFO] (run) Validating 31765 / 31765 blobs
[2025-03-30 01:50:41,262 INFO] (run) Blob validation done: total 31765, validated 31765, ok 31765, bad 0
[2025-03-30 01:50:41,263 INFO] (run) NICE, blob file fix done
```

### To check if everything is fixed

Run `!!pb database validate all` in game to validate everything in the database
