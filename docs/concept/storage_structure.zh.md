---
title: '存储结构'
---

# 存储结构

```mermaid
graph TB
    A[备份] --> B[基础文件集]
    A --> C[增量文件集]

    B --> D1[文件1]
    B --> D2[文件2]
    B --> D3[文件3]

    C --> D4["文件3(修改)"]
    C --> D5["文件4(新增)"]

    D1 --> E1[数据对象A]
    D2 --> E1
    D3 --> E2[数据对象B]
    D4 --> E3[数据对象C]
    D5 --> E4[数据对象D]

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

## 备份（Backup）

一个备份代表在特定时间点对备份目标的完整快照。每个备份都有唯一的 ID 作为其标识符

每个备份包含创建者信息、备份注释、备份时间等与备份相关的信息

每个备份都关联一个基础文件集和一个增量文件集，它们两个共同描述了这个备份所含的文件列表

## 文件集（Fileset）

文件集是备份的存储单元，采用基础文件集和增量文件集的组合模式

基础文件集（Base Fileset）：

- 包含一个完整的文件列表
- 存储文件的元数据和内容引用
- 可以被多个增量文件集引用

增量文件集（Delta Fileset）：

- 仅包含相对于基础文件集的变更
- 存储新增、修改、删除的文件信息
- 依赖基础文件集，不会独立存在

## 文件（File）

文件代表备份中的一个文件项，包含文件元数据和数据哈希

- 包含文件相对 [source_root](config.zh.md#source_root) 的 unix 风格路径
- 包含权限、所有者、时间戳等文件元信息
- 对于普通文件，仅储存其文件内容的哈希值
- 对于符号链接文件，直接储存其指向的路径
- 使用 role 字段标识其在文件集中的角色：
  - 独立文件：基础文件集中的完整文件
  - 覆盖文件：增量文件集中替换基础文件的文件
  - 新增文件：增量文件集中新增的文件
  - 删除标记：增量文件集中被删除的文件

## 数据对象（Blob）

数据对象（Blob）是实际存储文件内容的对象

- 使用哈希值作为其唯一标识符，一个哈希值有且仅有一个对应的数据对象
- 仅存储文件的内容数据及其压缩方式，不存储实际文件的元信息
- 具有两种存储方式：`direct` 和 `chunked`
  - `direct`（直存）数据对象会以独立文件的形式存储在 [storage_root](config.zh.md#storage_root) 下的 `blobs` 目录中
  - `chunked`（分块）数据对象不直接对应一个独立的 blob 文件，而是由多个数据块组和数据块按顺序重建出来；数据块文件则独立存放在 `blobs/_chunks` 目录中
- 一个数据对象可被多个文件对象引用。当引用数降为 0 时，PrimeBackup 会删除该数据对象

## 数据块与数据块组（Chunk and Chunk Group）

数据块（Chunk）是 CDC 为大文件引入的去重单位

- 一个数据块保存一段文件内容，以及它的哈希值、压缩方式和大小信息
- 数据块按内容定义的边界切分，因此即使大文件在中间插入或修改了数据，周围未变化的部分仍有机会落在相同的数据块中被复用
- 数据块文件会像直存数据对象（direct blob）一样独立存储，并在全局范围内去重

数据块组（Chunk Group）是一组按顺序组织的数据块，用于降低 chunked blob 的元数据展开规模

- Prime Backup 会将连续的数据块组织成数据块组，再按顺序将数据块组绑定回 blob
- 重建分块 blob 时，会先按顺序读取其数据块组，再按顺序读取每个组内的数据块
- 对于分块 blob，其 `stored_size` 表示所有唯一数据块存储大小之和，而非某个独立 blob 文件的大小

## 存储架构图

```mermaid
graph LR
    A[备份数据] --> DB[SQLite 数据库]
    A --> blob_pool[数据池]

    DB --> backup[备份对象]
    DB --> fileset[文件集对象]
    DB --> file[文件对象]
    DB --> blob[数据对象]
    DB --> chunk_group[数据块组对象]
    DB --> chunk[数据块对象]

    blob_pool --> blob_storage[直存 Blob 文件]
    blob_pool --> chunk_storage[数据块文件]

    style A fill:#e1f5fe
    style DB fill:#f3e5f5
    style blob_pool fill:#f3e5f5
    style backup fill:#e8f5e8
    style fileset fill:#e8f5e8
    style file fill:#e8f5e8
    style blob fill:#e8f5e8
    style chunk_group fill:#e8f5e8
    style chunk fill:#e8f5e8
    style blob_storage fill:#fff3e0
    style chunk_storage fill:#fff3e0
```
