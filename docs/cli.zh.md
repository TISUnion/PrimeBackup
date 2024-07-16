---
title: '命令行工具'
---

# 命令行工具

Prime Backup 的 `*.pyz` 文件除了是一个 MCDR 插件外，也是一个命令行界面（CLI）工具。
在安装了所需的 Python 依赖库后，你可以直接用 Python3 解析器来运行它

```
$ python3 PrimeBackup.pyz
usage: PrimeBackup.pyz [-h] [-d DB]
                       {overview,list,show,import,export,extract,migrate_db}   
                       ...

Prime Backup v1.7.0 CLI tools

options:
  -h, --help            show this help message and exit
  -d DB, --db DB        Path to the prime_backup.db database file, or path to  
                        the directory that contains the prime_backup.db        
                        database file, e.g. "/my/path/prime_backup.db", or     
                        "/my/path" (default: ./pb_files)

Command:
  {overview,list,show,import,export,extract,migrate_db}
                        Available commands
    overview            Show overview information of the database
    list                List backups
    show                Show detailed information of the given backup
    import              Import a backup from the given file. The backup file   
                        needs to have a backup metadata file
                        '.prime_backup.meta.json', or the --auto-meta flag     
                        need to be supplied
    export              Export the given backup to a single file
    extract             Extract a single file / directory from a backup
    migrate_db          Migrate the database to the current version (2)  
```

你可以在每个子命令后添加 `--help` 来显示其帮助信息。例如：

```
$ python3 PrimeBackup.pyz export --help
usage: PrimeBackup.pyz export [-h] [-f FORMAT] [--fail-soft] [--no-verify]
                              [--no-meta]
                              backup_id output

Export the given backup to a single file

positional arguments:
  backup_id             The ID of the backup to export. Besides an integer     
                        ID, it can also be "latest" and "latest_non_temp"      
  output                The output file name of the exported backup. Example:  
                        my_backup.tar

options:
  -h, --help            show this help message and exit
  -f FORMAT, --format FORMAT
                        The format of the output file. If not given, attempt   
                        to infer from the output file name. Options: tar,      
                        tar_gz, tar_bz2, tar_xz, tar_zst, zip
  --fail-soft           Skip files with export failure in the backup, so a     
                        single failure will not abort the export. Notes: a     
                        corrupted file might damaged the tar-based file        
  --no-verify           Do not verify the exported file contents
  --no-meta             Do not add the backup metadata file
                        '.prime_backup.meta.json' in the exported file
```

作为演示，下面的这个例子展示了数据库的状况概览：

```
$ python3 PrimeBackup.pyz -d run/pb_files overview
[2023-12-09 20:31:39,929 INFO] Storage root set to 'run/pb_files'
[2023-12-09 20:31:39,948 INFO] DB version: 1
[2023-12-09 20:31:39,948 INFO] Hash method: xxh128
[2023-12-09 20:31:39,948 INFO] Backup count: 2
[2023-12-09 20:31:39,948 INFO] Blob count: 9108
[2023-12-09 20:31:39,949 INFO] Blob stored size sum: 5158485126 (4.80GiB)
[2023-12-09 20:31:39,949 INFO] Blob raw size sum: 8295323157 (7.73GiB)
[2023-12-09 20:31:39,949 INFO] File count: 22010
[2023-12-09 20:31:39,949 INFO] File raw size sum: 16610326638 (15.47GiB)
```
