---
title: 'Database Operations'
---

Storage algorithm migration and backup data modification

## Hash Algorithm Migration

!!! warning

    - This affects all stored backups; make an extra copy of your data beforehand
    - Migration may take a long time depending on file count and disk I/O speed, and cannot be interrupted

Migrate the database to a new hash algorithm:

```
!!pb database migrate_hash_method <new_hash_algorithm>
```

TODO

## Compression Method Migration

!!! warning

    - This affects all stored backups; make an extra copy of your data beforehand
    - Migration may take a long time depending on file count and disk I/O speed, and cannot be interrupted

Migrate stored backup data to a new compression method:

```
!!pb database migrate_compress_method <new_compression_method>
```

TODO

## File Deletion

### Delete a File from a Backup

Delete a specific file from a backup:

```
!!pb database delete file <backup_id> <file_path>
```

Example:

```
!!pb database delete file 59 world/cache/file.txt
```

### Recursive Directory Deletion

Recursively delete a directory and all its contents from a backup:

```
!!pb database delete file <backup_id> <directory_path> --recursive
```

Example:

```
!!pb database delete file 59 world/cache --recursive
```

Notes:

- The backup root directory cannot be deleted
- Deleting a directory requires the `--recursive` option
- User confirmation is required by default to prevent accidental deletion
