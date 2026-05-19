# Prime Backup

English | [中文](README.zh.md)

A powerful backup plugin for MCDR, an advanced backup solution for your Minecraft world

Document: https://tisunion.github.io/PrimeBackup/

## Features

- Hash-based, compressed file pool deduplication; only new or changed data is stored, with no hard limit on backup count
- Optional blob chunking algorithm; supports CDC (content-defined chunking) for large, locally edited files to improve deduplication across backups
- Pack files store compressed binary entries such as chunk payloads, reducing small file pressure while supporting validation and compaction
- Safe restore workflow: confirmation + countdown, automatic pre-restore backup, recycle-bin rollback, and data verification
- Comprehensive backup operations, including backup/restore, list/delete, import/export, comments/tags, and more
- Smooth in-game interaction, with most operations achievable through mouse clicks
- Rich database toolkit: overview statistics, integrity validation, orphan cleanup, file deletion, and hash/compression method migration
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

For large and locally edited files, Prime Backup can optionally use CDC (Content-Defined Chunking) for better deduplication
The file is split into content-defined chunks; each chunk is hashed and reused across backups when unchanged, only new chunk payloads are written as pack entries

Prime Backup stores common file types, including regular files, directories, and symbolic links; for these 3 types:

- Regular file: Prime Backup calculates hashes (and size)
  If CDC is enabled, it stores the file as a chunked blob that references chunks; chunks are deduplicated individually, then new chunk payloads are compressed and stored as pack entries
  Otherwise, it stores a direct blob; the whole file is deduplicated and compressed as a single unit
  File metadata such as mode, uid, and mtime are stored in the database
- Directory: Prime Backup stores its information in the database
- Symlink: Prime Backup stores the symlink itself instead of the linked target

## Thanks

The idea for the hash-based file pool is inspired by https://github.com/z0z0r4/better_backup
