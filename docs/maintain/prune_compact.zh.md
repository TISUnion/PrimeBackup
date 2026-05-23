---
title: '"整理"相关概念辨析'
---

# "整理"相关概念辨析

Prime Backup 涉及多个具有"整理"性质的概念（pack / compact / vacuum），它们的作用层次、触发时机和实际效果各不相同

本文对这些概念作横向汇总，重点在于辨析它们之间的区别，帮助管理员快速把握全貌

## 概览

| 概念        | 英文名                 | 作用对象  | 自动触发             | 手动命令                          |
|-----------|---------------------|-------|------------------|-------------------------------|
| 备份修剪      | Backup Prune        | 备份    | 是（定时作业）          | `!!pb prune`                  |
| 数据库清理     | Database Prune      | 复合操作  | 否                | `!!pb database prune`         |
| 打包文件整理    | Pack Compaction     | 打包文件  | 是（数据块删除时 / 定时作业） | `!!pb database compact_packs` |
| SQLite 整理 | SQLite Vacuum       | 数据库文件 | 是（定时作业）          | `!!pb database vacuum`        |
| 基础文件集压缩   | Base Fileset Shrink | 基础文件集 | 是（备份删除时）         | （含于数据库清理）                     |
| 孤立对象清理    | Orphan Object Scan  | 数据库对象 | 否                | （含于数据库清理）                     |
| 未知文件清理    | Unknown File Scan   | 存储目录  | 否                | （含于数据库清理）                     |

---

## 备份修剪（Backup Prune）

作用于备份层，根据保留策略删除多余的旧备份

### 作用

按照配置的保留策略，逐个删除不再需要保留的备份，并级联释放这些备份独占的文件、数据对象等数据库对象及对应的物理存储

### 作用范围

备份按标签被划分为三类并分别裁量：常规备份 (regular)、定时备份 (scheduled)、临时备份 (temporary)

每类备份独立应用各自的 `PruneSetting` 配置；带有保护标签 (`is_protected = true`) 的备份无论如何都不会被修剪

### 保留决策流程

1. 使用 `last`、`hour`、`day`、`week`、`month`、`year` 按 [PBS 保留策略](https://pbs.proxmox.com/docs/prune-simulator/)选出每个时间桶中的代表性备份并标记保留
2. 对第一步标记保留的备份，用 `max_amount`（保留上限）和 `max_lifetime`（最大存活时长）进一步筛除过多或过期的备份
3. 删除所有未标记保留的备份

删除决策明细会记录至 `pb_files/logs/prune.log`

### 触发方式

- 自动：定时作业 `prune_backup`，由 `prune.interval`（默认 `6h`）或 `prune.crontab` 决定触发周期
- 手动：`!!pb prune`（需要权限 3）

---

## 数据库清理（Database Prune）

作用于多个存储层，执行一次完整的底层清理

### 作用

这是一个复合命令，它依次执行以下所有清理步骤：

1. 孤立对象清理（Orphan Object Scan）
2. 基础文件集压缩（Base Fileset Shrink）
3. 未知直存数据对象文件清理（Unknown Blob File Scan）
4. 打包文件整理（Pack Compaction），使用 `backup.pack_maintenance_compact_threshold`
5. 未知打包文件清理（Unknown Pack File Scan）

### 触发方式

- 手动：`!!pb database prune`（需要权限 4）

!!! note

    Prime Backup 在日常操作（如删除备份、备份修剪）中已能及时清理对应的数据，通常无需手动执行此命令

---

## 打包文件整理（Pack Compaction）

作用于打包文件层，通过重写消除打包文件中的"死空间"

### 作用

打包文件（Pack）以追加方式写入，数据块被删除后其占用的字节范围不会立即释放，成为死空间

整理时，超过死亡比例阈值的打包文件中的存活条目（live entries）会被重写到新的打包文件，旧文件随即删除；全部条目均已死亡的打包文件则直接删除

### 阈值

- `backup.pack_auto_compact_threshold`（默认 `0.5`）：即时触发时使用的存活比例下限；若被修改的打包文件中存活数据低于此比例则立即整理
- `backup.pack_maintenance_compact_threshold`（默认 `0.8`）：维护任务（定时作业和 `database prune`）使用的存活比例下限，阈值更宽松，可整理更多打包文件

### 触发方式

| 触发场景                          | 说明                                                                        |
|-------------------------------|---------------------------------------------------------------------------|
| 数据块被删除后（即时）                   | 若相关打包文件存活比例低于 `pack_auto_compact_threshold`，立即整理                          |
| 定时作业 `compact_pack`           | 默认 crontab `0 5 * * 0`（每周日 05:00），使用 `pack_maintenance_compact_threshold` |
| `!!pb database prune` 步骤 5    | 使用 `pack_maintenance_compact_threshold`                                   |
| `!!pb database compact_packs` | 阈值固定为 `1.0`，仅跳过全部存活的打包文件（需要权限 4）                                          |

---

## SQLite 整理（SQLite Vacuum）

作用于数据库文件层，整理 SQLite 数据库文件本身

### 作用

SQLite 在删除数据后不会立即缩减文件体积，而是在原位留下空洞；`VACUUM` 命令会重建数据库文件，消除空洞、整理碎片，从而缩小磁盘占用

此操作不修改任何备份数据或存储对象，仅影响 `prime_backup.db` 本身的文件大小

### 触发方式

- 自动：定时作业 `vacuum_sqlite`，默认 crontab `0 7 * * 0`（每周日 07:00）
- 手动：`!!pb database vacuum`（需要权限 4）

---

## 基础文件集压缩（Base Fileset Shrink）

作用于文件集层，清理基础文件集中的冗余文件条目

### 作用

文件集采用基础+增量（base + delta）的组合结构；基础文件集中的某个文件，如果已被所有引用它的增量文件集完全覆写或删除，那么它在基础文件集中的条目就属于冗余

压缩操作会：

- 从基础文件集中移除这些冗余文件条目
- 对于原本标记为"覆写（delta_override）"的增量条目，改为"新增（delta_add）"，以便后续独立存活
- 对于原本标记为"删除（delta_remove）"的增量条目，由于基础条目已消失，删除标记也随之变得无意义，一并移除

### 触发方式

- 自动：删除备份时，若该备份所属的基础文件集仍被其他备份共享，则自动对该基础文件集执行一次压缩
- 间接：`!!pb database prune` 步骤 2（`ShrinkAllBaseFilesetsAction`，扫描所有基础文件集）

!!! note

    日常的备份删除流程已经会及时清理文件集中的冗余文件条目，正常情况下此扫描不会发现任何冗余对象

---

## 孤立对象清理（Orphan Object Scan & Delete）

作用于数据库对象层，扫描并删除不再被任何上层对象引用的"孤立"数据库记录

### 孤立对象的来源

正常的删除流程会级联清理对应对象，但在极少数情况（如意外中断、并发异常）下可能残留孤立对象

### 清理内容

依次扫描并删除以下对象：

| 对象                         | 判断依据                                                       |
|----------------------------|------------------------------------------------------------|
| 孤立文件集（Orphan Fileset）      | 不被任何备份引用的文件集                                               |
| 孤立文件（Orphan File）          | 不被任何文件集引用的文件对象                                             |
| 孤立数据对象（Orphan Blob）        | 不被任何文件引用的数据对象                                              |
| 孤立数据块组（Orphan Chunk Group） | 不被任何数据对象引用的数据块组                                            |
| 孤立数据块（Orphan Chunk）        | 不被任何数据块组引用的数据块                                             |
| 孤立关系绑定（Orphan Binding）     | 仅针对 Blob-ChunkGroup 和 ChunkGroup-Chunk 两种绑定表，删除指向不存在对象的绑定行 |

### 触发方式

- 间接：`!!pb database prune` 步骤 1

!!! note

    日常的备份删除流程已经会及时清理对应的孤立对象，正常情况下此扫描不会发现任何孤立对象

---

## 未知文件清理（Unknown File Scan & Delete）

作用于文件系统层，扫描并删除存储目录中无对应数据库记录的多余文件

### 作用

Prime Backup 在回滚失败或意外中断时可能在存储目录中遗留临时文件，这些文件不在数据库记录中，也不会被正常流程清理

### 清理范围

| 存储目录              | 说明                        |
|-------------------|---------------------------|
| `pb_files/blobs/` | 扫描直存数据对象存储目录，删除不存在于数据库的文件 |
| `pb_files/packs/` | 扫描打包文件存储目录，删除不存在于数据库的文件   |

### 触发方式

- 间接：`!!pb database prune` 步骤 3、5
