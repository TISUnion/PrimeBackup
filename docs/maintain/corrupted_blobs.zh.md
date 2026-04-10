---
title: '数据文件损坏的处理'
---

# 数据文件损坏的处理

本文档介绍当 Prime Backup 存储池中的数据对象文件（blob）或数据块文件（chunk）发生损坏时，如何确认问题、临时绕过回档失败，以及如何修复损坏

## 背景知识

Prime Backup 将备份数据以数据对象（Blob）的形式持久化到文件系统中。有两种存储方式：

- 直存（direct）：每个 blob 对应一个独立文件，存储路径为 `pb_files/blobs/{哈希前2位}/{完整哈希}`，
  例如：`pb_files/blobs/8b/8b36d71e250e25527f59b8fd9e0f2dce`
- 分块（chunked）：大文件经 CDC 算法切分成多个数据块（chunk），各 chunk 分别独立存储在 `pb_files/blobs/_chunks/{哈希前2位}/{完整哈希}` 路径下，
  例如：`pb_files/blobs/_chunks/3a/3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5`

当磁盘故障、文件系统错误、意外断电或误操作等原因导致这些文件内容被破坏或丢失时，PB 在回档（或导出）时将无法正常还原对应文件，进而导致操作失败

> 关于存储结构的详细说明，请参阅[存储结构](../concept/storage_structure.zh.md)

## 问题的表现

### 回档失败

默认情况下，PB 在回档时会对每个还原后的文件进行哈希校验（`verify_blob=True`）。若 blob 或 chunk 文件缺失、损坏，或哈希/大小与数据库记录不符，回档将报错中止并回滚所有已写入的文件

场景一：直存 blob 文件损坏（哈希不匹配）

```
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e250e25527f59b8fd9e0f2dce, actual written 9f4c7b3e5d1a2c8e6f0b4d9e2a7f3c1d
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: Error occurs during export to directory, applying rollback
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Restore to backup #78 failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e250e25527f59b8fd9e0f2dce, actual written 9f4c7b3e5d1a2c8e6f0b4d9e2a7f3c1d
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: The server is left stopped due to the failure. Prime Backup will not attempt to restart it automatically
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: You can try to fix the issue and perform another restoration, or just start the server manually via MCDR command
```

场景二：分块 blob 的某个 chunk 文件损坏（哈希不匹配）

```
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (chunk 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5), expected 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5, actual written ff92dcb10e3a7b5c2d4f8e1a9c6b0d3f
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: Error occurs during export to directory, applying rollback
[MCDR] [22:09:30] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Restore to backup #78 failed: hash mismatched for world/region/r.0.0.mca (chunk 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5), expected 3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5, actual written ff92dcb10e3a7b5c2d4f8e1a9c6b0d3f
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: The server is left stopped due to the failure. Prime Backup will not attempt to restart it automatically
[MCDR] [22:09:30] [PB@ecac-worker-heavy/WARN] [prime_backup]: You can try to fix the issue and perform another restoration, or just start the server manually via MCDR command
```

!!! warning

    回档失败后，服务器进程将保持停止状态，PB 不会自动重启它  
    你需要先处理损坏问题，或使用 MCDR 命令手动启动服务器：`!!MCDR start`

由于 PB 在失败时会将回档目标目录回滚到原始状态，因此回档失败不会破坏现有的服务器文件

## 扫描与确认问题

### 一键检查（`validate all`）

使用以下命令可以一次性验证数据库中所有组件的正确性：

```
!!pb database validate all
```

该命令依次验证：数据对象（blobs）、数据块相关对象（chunks）、文件对象（files）、文件集（filesets）、备份（backups）

输出示例（全部正常）：

```
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO]: [PB] 正在验证所有数据对象, 请稍等...
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:51] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 全部4325个数据对象都是健康的
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 正在验证所有数据块相关对象 (包括数据块、数据块组、关系绑定), 请稍等...
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 8765, validated 8765, ok 8765, bad 0
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO]: [PB] 全部8765个数据块、...个数据块组、... + ...个数据块关系绑定对象都是健康的
[MCDR] [23:15:57] [PB@xxx-worker-heavy/INFO]: [PB] ...（files/filesets/backups 验证略）...
[MCDR] [23:16:12] [PB@xxx-worker-heavy/INFO]: [PB] 验证完成, 耗时34.21s。数据对象: 健康, 数据块相关对象: 健康, 文件对象: 健康, 文件集: 健康, 备份: 健康
```

!!! note

    本文档仅涉及 blob/chunk 文件层面的损坏处理（对应 `blobs` 和 `chunks` 两部分的验证结果）
    `files`、`filesets`、`backups` 等组件若出现异常，属于数据库结构层面的问题，处理方式不在本文档范围内

### 扫描直存 blob 文件

使用以下命令对所有数据对象进行全面扫描：

```
!!pb database validate blobs
```

正常情况的输出：

```
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO]: [PB] 正在验证所有数据对象, 请稍等...
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:38] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:15:51] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 全部4325个数据对象都是健康的
[MCDR] [23:15:53] [PB@xxx-worker-heavy/INFO]: [PB] 验证完成, 耗时15.32s。数据对象: 健康
```

发现异常时的输出（示例）：

```
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO]: [PB] 正在验证所有数据对象, 请稍等...
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4322, bad 3
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 发现了3/4325个异常数据对象
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 文件缺失的数据对象: 2个
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 1. 4fc02ad5e7508949634f012fb96c2968: blob file /path/to/pb_files/blobs/4f/4fc02ad5e7508949634f012fb96c2968 does not exist
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 2. 54695a153daae89e685894ee966a277e: blob file /path/to/pb_files/blobs/54/54695a153daae89e685894ee966a277e does not exist
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 信息不匹配的数据对象: 1个
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 1. 8b36d71e250e25527f59b8fd9e0f2dce: stored size mismatch, expect 786432, found 786428
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 影响范围: 6/9147个文件对象, 2/26个文件集, 16/21个备份
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 见日志文件 pb_files/logs/validate.log 以了解这些数据对象的详细信息及影响范围
[MCDR] [23:19:06] [PB@xxx-worker-heavy/INFO]: [PB] 验证完成, 耗时6.34s。数据对象: 异常
```

#### 异常类型说明

| 类型键          | 中文名         | 含义                                           |
|--------------|-------------|----------------------------------------------|
| `missing`    | 文件缺失的数据对象   | blob 文件在存储路径下不存在                             |
| `corrupted`  | 文件损坏的数据对象   | blob 文件存在，但解压缩失败                             |
| `mismatched` | 信息不匹配的数据对象  | 解压后的哈希值或大小与数据库记录不符                           |
| `bad_layout` | 分块布局异常的数据对象 | chunked blob 的数据块组绑定关系异常                     |
| `invalid`    | 非法数据对象      | blob 的压缩方式等基本字段非法                            |
| `orphan`     | 孤儿数据对象      | blob 没有被任何文件引用（不影响回档，可用 `database prune` 清理） |

前三种（`missing`、`corrupted`、`mismatched`）是最常见的"文件层面"损坏，会直接导致回档失败

#### 查阅详细日志

输出末尾会提示日志文件路径（`pb_files/logs/validate.log`），其中记录了完整的受影响备份 ID 列表和受影响文件样本，建议优先查阅：

```
Affected file objects / total file objects: 6 / 9147
Affected file samples (len=3):
- FileInfo(fileset_id=12, path='world/region/r.0.0.mca', ...)
- FileInfo(fileset_id=15, path='world/region/r.0.0.mca', ...)
- FileInfo(fileset_id=18, path='world/region/r.0.0.mca', ...)
Affected backup / total backups: 16 / 21
Affected backup IDs (bad blobs): [3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33]
```

### 扫描分块 blob 的 chunk 文件

使用以下命令对所有数据块及其关联对象（数据块组、绑定关系）进行扫描：

```
!!pb database validate chunks
```

发现 chunk 损坏时的输出（示例）：

```
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO]: [PB] 正在验证所有数据块相关对象 (包括数据块、数据块组、关系绑定), 请稍等...
[MCDR] [23:19:00] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation start
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO] [prime_backup]: Validating 8765 / 8765 chunks
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 8765, validated 8765, ok 8763, bad 2
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 发现了2/8765个异常数据块
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 文件损坏的数据块: 2个
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 1. id=1234 hash=3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5: cannot read and decompress chunk file: (<class 'zlib.error'> Error -5 while decompressing data: incomplete or truncated stream
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 2. id=5678 hash=9b4c3d7e2f1a0b6c8e7d5c3a2b1f9e0d: cannot read and decompress chunk file: (<class 'zlib.error'> Error -5 while decompressing data: incomplete or truncated stream
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 影响范围: 3/9147个文件对象, 1/26个文件集, 8/21个备份
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 见日志文件 pb_files/logs/validate.log 以了解这些数据对象的详细信息及影响范围
[MCDR] [23:19:04] [PB@xxx-worker-heavy/INFO]: [PB] 验证完成, 耗时4.56s。数据块相关对象: 异常
```

chunk 的损坏类型与 blob 类似，包括：`missing_file`（文件缺失）、`corrupted`（解压失败）、`mismatched`（哈希/大小不匹配）

由于一个 chunk 可被多个 chunked blob 复用，一个 chunk 损坏可能影响多个备份中的多个文件

### 进一步定位受影响的文件和备份

通过 `!!pb database inspect` 系列命令可以深入排查受影响的对象：

审查指定数据对象（blob）：

```
!!pb database inspect blob 8b36d71e250e25527f59b8fd9e0f2dce
```

输出示例：

```
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] ======== 数据对象8b36d71e250e2552 ========
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] ID: 42
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 存储方法: 直存
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 哈希: 8b36d71e250e25527f59b8fd9e0f2dce (xxh128)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 压缩方法: zstd
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 原始大小: 1048576 (1.00MiB)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 储存大小: 786432 (768.00KiB)
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 关联文件数: 6。样本：
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 1. 文件集12, 路径: world/region/r.0.0.mca
[MCDR] [22:11:00] [PB@xxx-worker-light/INFO]: [PB] 2. 文件集15, 路径: world/region/r.0.0.mca
```

审查指定备份的某个文件：

```
!!pb database inspect file 78 world/region/r.0.0.mca
```

审查指定备份：

```
!!pb database inspect backup 78
```

## 临时绕过措施

!!! warning

    以下措施仅用于临时应急，并不能修复损坏的数据。使用后回档出的备份内容将不完整，相关游戏文件（如存档）可能已损坏。请在充分了解风险后谨慎使用

### 跳过失败的文件（`--fail-soft`）

在回档命令中加入 `--fail-soft` 参数，PB 会捕获并记录每个文件的导出错误，跳过失败的文件并继续回档过程：

```
!!pb back 78 --fail-soft
```

`--fail-soft` 适用于所有类型的导出失败（包括文件缺失、解压失败、哈希不匹配）。回档完成后，PB 会在日志中列出所有跳过的文件：

```
[MCDR] [22:10:12] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.0.0.mca' to path world/region/r.0.0.mca failed: hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e..., actual written 9f4c7b3e...
[MCDR] [22:10:12] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Export file 'world/region/r.1.0.mca' to path world/region/r.1.0.mca failed: hash mismatched for world/region/r.1.0.mca (chunk 3a7f29c4...), expected 3a7f29c4..., actual written ff92dcb1...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: Export done with 2 failures
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: Found 2 failures during backup export
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: world/region/r.0.0.mca mode=0o100644: (VerificationError) hash mismatched for world/region/r.0.0.mca (blob), expected 8b36d71e..., actual written 9f4c7b3e...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/ERROR] [prime_backup]: world/region/r.1.0.mca mode=0o100644: (VerificationError) hash mismatched for world/region/r.1.0.mca (chunk 3a7f29c4...), expected 3a7f29c4..., actual written ff92dcb1...
[MCDR] [22:10:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restore to backup #78 done, cost 10.32s (backup 2.41s, restore 7.91s), starting the server
```

!!! warning

    使用 `--fail-soft` 时，跳过失败的文件后，这些文件在回档目录中的最终状态取决于错误类型：

    - 文件缺失（`missing`/`missing_file`）：blob/chunk 文件不存在，无法写入，目标路径上将不存在该文件
    - 文件损坏（`corrupted`）：解压过程中发生异常，目标文件可能以截断/部分写入的形式存在
    - 哈希不匹配（`mismatched`）：文件被完整写入，但内容来自损坏的数据，内容不正确

    此外，由于回档在开始时会清空回档目标目录，失败文件的回档前内容也已被删除，无法自动恢复。如需恢复需手动还原至回档前的备份（如有创建）

### 跳过哈希校验（`--no-verify`）

在回档命令中加入 `--no-verify` 参数，PB 会跳过对还原文件的哈希/大小校验：

```
!!pb back 78 --no-verify
```

`--no-verify` 适用于 blob/chunk 文件内容可以正常读取和解压，但哈希值或大小与数据库记录不符的情况（即 `mismatched` 类型）
对于文件缺失或无法解压的情况，`--no-verify` 无效，仍需配合 `--fail-soft` 使用

两个参数可以组合使用：

```
!!pb back 78 --fail-soft --no-verify
```

## 修复方法

### 不应该做的事

!!! danger

    **不要直接删除 blob 或 chunk 文件**

    直接在文件系统中删除 `pb_files/blobs/` 或 `pb_files/blobs/_chunks/` 下的文件，会使数据库中的记录与文件系统不一致
    PB 并不知道文件已被删除，后续的回档、导出等操作仍会尝试读取不存在的文件，导致各种难以排查的错误

!!! danger

    **不要直接修改 SQLite 数据库**

    PrimeBackup 的数据库（`pb_files/prime_backup.db`）有复杂的关系结构（blob、chunk、chunk_group、binding 等多层关联）
    手动修改很容易破坏引用完整性，导致数据库进入难以恢复的损坏状态

### 方法一：从外部备份恢复 blob/chunk 文件

适用场景： 你持有 `pb_files` 目录的外部备份（如操作系统级别的文件备份），其中包含完好的 blob 或 chunk 文件

操作步骤：

1. 确认需要恢复的文件路径（从 validate 输出中获取哈希值）：
   - 直存 blob：`pb_files/blobs/{哈希前2位}/{完整哈希}`  
     例如：`pb_files/blobs/8b/8b36d71e250e25527f59b8fd9e0f2dce`
   - chunk 文件：`pb_files/blobs/_chunks/{哈希前2位}/{完整哈希}`  
     例如：`pb_files/blobs/_chunks/3a/3a7f29c4b5e1d8a0f6c2e9b7d4a1f8c5`

2. 从外部备份中将对应文件复制回原路径，覆盖损坏的文件（或在文件缺失时直接放入）

3. 再次运行 `!!pb database validate blobs`（或 `validate chunks`）确认问题已修复

!!! tip

    恢复前可用 `!!pb database inspect blob <哈希>` 确认该 blob 的存储方式（直存/分块）和压缩方式，以便确认外部备份文件是否匹配

### 方法二：删除受影响的备份

适用场景： 损坏的 blob/chunk 所对应的备份已不再需要，或者你宁愿丢弃这些备份而不是保留不完整的数据

操作步骤：

1. 从 validate 输出或日志文件中获取受影响的备份 ID 列表

2. 逐个删除受影响的备份：

    ```
    !!pb delete <备份ID>
    ```

    例如，删除备份 #3、#5、#7：

    ```
    !!pb delete 3 5 7
    ```

    或使用范围删除（如受影响备份 ID 连续）：

    ```
    !!pb delete_range 3-7
    ```

3. 删除备份后，PB 会自动清理不再被任何备份引用的孤儿 blob 和 chunk（包括删除其对应的物理文件）。如果仍有残留，可手动执行：

    ```
    !!pb database prune
    ```

!!! note

    删除备份时，如果某个 blob/chunk 仍被其他未删除的备份引用，它不会被自动删除，相应的损坏数据依然存在于存储池中，只是不再影响被删除的备份
    因此，若要彻底清除损坏数据，你需要删除所有引用它的备份

!!! tip

    受影响的备份 ID 可以从 validate 命令的游戏内输出直接点击，也可以从 `validate.log` 日志文件中完整获取

### 方法三：（未实现）提供包含完好数据的文件以重建 blob/chunk

!!! info

    此功能尚未实现

从理论上讲，如果你持有一个或多个与受损 blob/chunk 内容相同的文件，可以通过专用修复命令让 PB 重新入库这些文件，从而修复损坏的存储条目

计划中的大致使用方式：提供一个或多个文件路径（或包含这些文件的压缩包），PB 会对每个输入文件计算哈希值，
并在数据库中查找具有匹配哈希且处于损坏状态（`missing`、`corrupted`、`mismatched`）的 blob 或 chunk 条目。若找到，则将该文件重新写入到对应存储路径，完成修复

当前版本的 PrimeBackup 尚不支持此操作：`!!pb import` 命令在处理文件时，若数据库中已存在相同哈希的 blob 记录，不会重新写入对应的物理文件，
因此即使导入包含相同内容的备份，损坏的 blob 文件也不会被替换

如需此功能，请关注后续版本的更新

## 总结

| 问题类型                                      | 推荐处理方式                                                                                               |
|-------------------------------------------|------------------------------------------------------------------------------------------------------|
| blob/chunk 文件缺失（`missing`/`missing_file`） | 从外部备份恢复文件，或删除受影响备份                                                                                   |
| blob/chunk 文件损坏，无法解压（`corrupted`）         | 从外部备份恢复文件，或删除受影响备份                                                                                   |
| 哈希/大小不匹配（`mismatched`）                    | 从外部备份恢复文件，或删除受影响备份；临时可用 `--no-verify` 绕过                                                             |
| 临时需要回档某个受损的备份                             | `!!pb back <ID> --fail-soft`（跳过损坏文件）或 `--no-verify`（跳过哈希校验）                                          |
| 想确认损坏的范围                                  | `!!pb database validate blobs` / `validate chunks` / `validate all`，并查阅 `pb_files/logs/validate.log` |
