---
title: 'Database Operations'
---

Configuration migration and data modification operations.

## Hash Algorithm Migration

!!! warning

    - Affects all backup content, it's recommended to make additional data backups
    - Migration may take a long time depending on file count and disk IO speed. Migration tasks cannot be interrupted

Migrate the hash algorithm in the database to a new algorithm:

```
!!pb database migrate_hash_method <new_hash_algorithm>
```

TODO

## Compression Method Migration

!!! warning

    - Affects all backup content, please make additional data backups
    - Migration may take a long time depending on file count and disk IO speed. Migration tasks cannot be interrupted

```
!!pb database migrate_compress_method <new_compression_method>
```

TODO

## File Deletion

### Delete Files from Backup

Delete specific files from a backup:

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

- Cannot delete the root directory of the backup
- Deleting directories requires the `--recursive` parameter
- Default requires user confirmation to prevent accidental operation
