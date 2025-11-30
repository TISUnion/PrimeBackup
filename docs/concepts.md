---
title: 'Core Concepts'
---

# PrimeBackup Core Concepts

This document introduces the core concepts of PrimeBackup

## Storage Structure

```mermaid
graph TB
    A[Backup] --> B[Base Fileset]
    A --> C[Delta Fileset]

    B --> D1[File1]
    B --> D2[File2]
    B --> D3[File3]

    C --> D4["File3 (Modified)"]
    C --> D5["File4 (Added)"]

    D1 --> E1[Blob A]
    D2 --> E1
    D3 --> E2[Blob B]
    D4 --> E3[Blob C]
    D5 --> E4[Blob D]

    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#f3e5f5
    style D1 fill:#e8f5e8
    style D2 fill:#e8f5e8
    style D3 fill:#e8f5e8
    style D4 fill:#e8f5e8
    style D5 fill:#e8f5e8
    style E1 fill:#fff3e0
    style E2 fill:#fff3e0
    style E3 fill:#fff3e0
    style E4 fill:#fff3e0
```

### Backup

A backup represents a complete snapshot of the backup target at a specific point in time. Each backup has a unique ID as its identifier

Each backup contains information related to the backup such as creator information, backup notes, backup time, etc.

Each backup is associated with a base fileset and a delta fileset, which together describe the list of files contained in this backup

### Fileset

Fileset is the storage unit of backups, using a combination of base fileset and delta fileset

Base Fileset:

- Contains a complete file list
- Stores file metadata and content references
- Can be referenced by multiple delta filesets

Delta Fileset:

- Only contains changes relative to the base fileset
- Stores information about added, modified, and deleted files
- Depends on the base fileset and does not exist independently

### File

File represents a file item in a backup, containing file metadata and data hash

- Contains the Unix-style path of the file relative to [source_root](config.md#source_root)
- Contains file metadata such as permissions, owner, timestamps, etc.
- For regular files, only stores the hash value of their file content
- For symbolic link files, directly stores the path they point to
- Uses the role field to identify its role in the fileset:
  - Independent file: Complete file in the base fileset
  - Override file: File in the delta fileset that replaces the base file
  - Added file: Newly added file in the delta fileset
  - Delete marker: File that has been deleted in the delta fileset

### Blob

Blob is the actual storage object for file content

- Uses hash value as its unique identifier, one hash value has exactly one corresponding blob
- Only stores file content data and its compression method, does not store actual file metadata
- Stored independently as files, located in the blobs folder under the [storage_root](config.md#storage_root) path
- One blob can be referenced by multiple file objects. When the reference count drops to 0, PrimeBackup will delete this blob

### Storage Architecture Diagram

```mermaid
graph LR
    A[Backup Data] --> DB[SQLite Database]
    A --> blob_pool[Blob Pool]

    DB --> backup[Backup Objects]
    DB --> fileset[Fileset Objects]
    DB --> file[File Objects]
    DB --> blob[Blob Objects]

    blob_pool --> blob_storage[Hash Sharding]
    blob_storage --> blob_file[Blob Files]

    style A fill:#e1f5fe
    style DB fill:#f3e5f5
    style blob_pool fill:#f3e5f5
    style backup fill:#e8f5e8
    style fileset fill:#e8f5e8
    style file fill:#e8f5e8
    style blob fill:#e8f5e8
    style blob_storage fill:#fff3e0
    style blob_file fill:#fff3e0
```