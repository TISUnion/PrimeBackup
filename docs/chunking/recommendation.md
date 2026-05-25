---
title: 'Chunking Recommendations'
---

This page lists common chunking rule recommendations for Minecraft save scenarios

Not all of these rules are suitable for the default configuration. The default configuration should stay conservative
and only cover file types with stable benefits, few dependencies, and low false-positive risk

## Recommendation Table

| File type               | patterns                                             | Recommended algorithm | Threshold | Rating |
|-------------------------|------------------------------------------------------|-----------------------|-----------|--------|
| Minecraft Anvil files   | `*.mca`                                              | `fixed_auto`          | `256 KiB` | ★★★★★  |
| Log files               | `*.log`                                              | `fixed_128k`          | `10 MiB`  | ★★★★★  |
| SQLite main databases   | `*.db`, `*.sqlite`, `*.sqlite3`                      | `fastcdc_32k`         | `20 MiB`  | ★★★☆☆  |
| SQLite WAL files        | `*.db-wal`, `*.sqlite-wal`, `*.sqlite3-wal`, `*.wal` | `fixed_128k`          | `10 MiB`  | ★★★☆☆  |
| JSONL record files      | `*.jsonl`                                            | `fixed_128k`          | `10 MiB`  | ★★★☆☆  |
| JSON / YAML state files | `*.json`, `*.yaml`, `*.yml`                          | `fastcdc_32k`         | `20 MiB`  | ★★☆☆☆  |

## Minecraft Anvil Files

Minecraft world data such as region, entities, and poi data is usually stored in `.mca` files

These files are internally organized around 4 KiB pages. When the world is running,
only some chunks, entities, or POI data change, so many pages from old backups may be reusable

`fixed_auto` is recommended. It uses 128 KiB as the default granularity, then falls back to 4 KiB granularity after detecting changed windows,
which provides a reasonable balance between metadata overhead and reuse quality

Example configuration:

```json
{
    "algorithm": "fixed_auto",
    "file_size_threshold": 262144,
    "patterns": [
        "*.mca"
    ]
}
```

## SQLite Main Databases

Many plugins use SQLite to store permissions, economy data, records, map caches, or other structured data

Common suffixes for main database files include `.db`, `.sqlite`, and `.sqlite3`.
These files are usually not pure append-write files; database pages may be modified, moved, or rewritten in the middle of the file

`fastcdc_32k` is recommended. CDC determines chunk boundaries from content, so it adapts better than fixed-size chunking to local changes inside databases

This rule is not recommended for the default configuration, because it introduces the optional `pyfastcdc` dependency,
and not every `.db` file is large enough or has stable reuse benefit

Example configuration:

```json
{
    "algorithm": "fastcdc_32k",
    "file_size_threshold": 20971520,
    "patterns": [
        "*.db",
        "*.sqlite",
        "*.sqlite3"
    ]
}
```

## SQLite WAL Files

When SQLite runs in WAL mode, new writes are first recorded into WAL files, with common suffixes such as `.db-wal`, `.sqlite-wal`, `.sqlite3-wal`, and `.wal`

The write pattern of WAL files is usually close to tail append, so fixed-size chunking can steadily reuse earlier old content while avoiding the CDC dependency

`fixed_128k` is recommended. Compared with 4 KiB or 32 KiB granularity, it can significantly reduce the number of chunks and is safer for large WAL files

Note that WAL files may be truncated or recreated after checkpointing. If a WAL file is often reset completely between backups, chunking benefit will decrease

Example configuration:

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.db-wal",
        "*.sqlite-wal",
        "*.sqlite3-wal",
        "*.wal"
    ]
}
```

## Log Files

Log files usually end with `.log`, and are common in the server itself, MCDR, plugins, or mods

Large logs are typically appended at the tail. For this kind of file,
fixed-size chunk boundaries for old chunks are not affected by appended content, so existing content can keep being reused

`fixed_128k` is recommended. It does not require CDC, and its metadata overhead is much lower than smaller fixed-size chunks

Example configuration:

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.log"
    ]
}
```

## JSONL Record Files

Some plugins or helper tools use JSONL to record events, statistics, or history data, where each line is usually an independent record

If the file mainly grows by appending records, it behaves similarly to log files and is suitable for fixed-size chunking

`fixed_128k` is recommended. It can steadily reuse old content while avoiding the optional CDC dependency

Example configuration:

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.jsonl"
    ]
}
```

## JSON / YAML State Files

`.json`, `.yaml`, and `.yml` files are common in the plugin ecosystem, but most configuration files are small and not worth chunking

Chunking is only worth considering when these files become large state files.
For example, some plugins write player data, claim data, statistics, or cache data into a single large text file

`fastcdc_32k` is recommended. When JSON / YAML is rewritten, insertions, deletions, or field changes may shift later content,
and CDC can preserve reusable regions better than fixed-size chunking

Use this kind of rule carefully. If the file reorders fields, refreshes many timestamps, or rewrites the whole file every time it is saved, chunking benefit is usually unstable

Example configuration:

```json
{
    "algorithm": "fastcdc_32k",
    "file_size_threshold": 20971520,
    "patterns": [
        "*.json",
        "*.yaml",
        "*.yml"
    ]
}
```

## Poor Candidates

The following files are usually not suitable as chunking rule targets:

- compressed files, such as `.gz`, `.zip`, `.zst`
- archives or packages, such as `.jar`
- images, map tiles, or other media files
- many very small configuration files
- files that are randomly rewritten as a whole on every save

Even if these files are chunked, they usually only increase the overhead of hashing, database records, and pack entries
