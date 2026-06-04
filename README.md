# Prime Backup

English | [中文](README.zh.md)

A powerful backup plugin for MCDR, an advanced backup solution for your Minecraft world

Document: https://tisunion.github.io/PrimeBackup/

## Features

- Hash-based, compressed file pool deduplication; only new or changed data is stored, with no hard limit on backup count
- Optional file chunking algorithm; supports Fixed-Size Chunking, Content-Defined Chunking, and other algorithms, splitting files into chunks for hash-based deduplication to further improve storage efficiency
- Pack files store compressed binary entries such as chunk payloads, reducing small file pressure while supporting validation and compaction
- Safe restore workflow: confirmation + countdown, automatic pre-restore backup, recycle-bin rollback, and data verification
- Comprehensive backup operations, including backup/restore, list/view, diff display, import/export, and more
- Smooth in-game interaction, with most operations achievable through mouse clicks
- Rich database toolkit: object query, database overview, data integrity validation, orphan cleanup, backup file deletion, and hash/compression method migration
- Highly customizable backup pruning strategies, similar to the strategy used by [PBS](https://pbs.proxmox.com/docs/prune-simulator/)
- Scheduled jobs for automatic backup creation and backup pruning, support fixed intervals and crontab expressions
- Provides a command-line tool if you want to manage backups without MCDR, and supports mounting as a filesystem via FUSE

![!!pb command](docs/img/pb_welcome.png)

## Requirements

[MCDReforged](https://github.com/Fallen-Breath/MCDReforged) requirement: `>=2.12.0`

Python package requirements: See [requirements.txt](requirements.txt)

## Usages

See the document: https://tisunion.github.io/PrimeBackup/

## How it works

Prime Backup maintains a custom file pool to store backup data, and every stored object is identified by a hash of its content
With that, Prime Backup can deduplicate files with the same content, and only stores 1 copy of them, greatly reducing disk usage

Prime Backup also supports compression on stored data to further reduce disk usage

For large and locally edited files, Prime Backup can optionally enable file chunking for better deduplication.
The file is split into multiple chunks, and each chunk is hashed.
If a chunk's content hasn't changed, it can be reused across different backups, with only new chunk payloads written as pack entries

Prime Backup stores common file types, including regular files, directories, and symbolic links; for these 3 types:

- Regular file: Prime Backup calculates hashes (and size)
  If chunking is enabled, it stores the file as a chunked blob that references chunks; chunks are deduplicated individually, then new chunk payloads are compressed and stored as pack entries
  Otherwise, it stores a direct blob; the whole file is deduplicated and compressed as a single unit
  File metadata such as mode, uid, and mtime are stored in the database
- Directory: Prime Backup stores its information in the database
- Symlink: Prime Backup stores the symlink itself instead of the linked target

## Thanks

The idea for the hash-based file pool is inspired by https://github.com/z0z0r4/better_backup
