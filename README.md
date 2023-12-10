# Prime Backup

A powerful backup plugin for MCDR, an advanced backup solution for your Minecraft world

Document: [https://tisunion.github.io/PrimeBackup/](https://tisunion.github.io/PrimeBackup/)

## Features

特性：

- 基于哈希的去重文件池
- 完善的备份操作
- 流畅的游戏内交互
- 定时任务
- 高可自定义的备份清理
- 命令行工具



- Hash-based file pool
- Backup operations
  - create, restore, delete
  - list, show, tagging (hidden, protected)
  - export, import
- Crontab job
  - Scheduled backup
  - Automatic backup prune
  - Database compact and backup
- CLI tools

## How it works

Prime Backup maintains a custom file pool to store the backup files. 
Every file in the pool is identified with the hash value of its content.
With that, Prime Backup can deduplicate files with same content, and only stores 1 copy of them, 
greatly reduced the burden on disk usage. 

Besides that, Prime Backup also supports compression on the stored files, 
which reduces the disk usage further more

Prime Backup also save the detailed file status of stored files, e.g. mode, uid, and, mtime

### Backup creation

PrimeBackup can store common file types, including regular files, directories, and symbolic links.
PrimeBackup stores them with the following logic:

- Regular file: Prime Backup calculates its hash values, 
  stores its content into the blob storage if the hash does not exist yet.
  For its file status, including mode, uid, mtime etc., will be stored in the database
- Directory: Prime Backup stores its information in the database
- Symlink: Prime Backup stores the symlink itself, instead of the linked target

### Backup retention

Prime Backup provides an advanced backup prune feature to automatically clean up old backups.

You can define the max amount of backups to keep, max lifetime for backups to keep,
or even creates a detailed [PBS-style](https://pbs.proxmox.com/docs/prune-simulator/)
backup pruning strategy, to precisely control the way to prune / retain backups

## Requirements

[MCDReforged](https://github.com/Fallen-Breath/MCDReforged) requirement: `>=2.12.0`

Python package requirements: See [requirements.txt](requirements.txt)

## Usages

See the document: [https://tisunion.github.io/PrimeBackup/](https://tisunion.github.io/PrimeBackup/)

![!!pb command](docs/img/pb_welcome.png)

## Thanks

The idea for the hash-based file pool is inspired by https://github.com/z0z0r4/better_backup
