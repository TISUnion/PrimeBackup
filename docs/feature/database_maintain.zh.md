---
title: '数据库维护'
---

一致性校验与数据库维护

## 数据库验证

| 命令                                | 说明                         |
|-----------------------------------|----------------------------|
| `!!pb database validate packs`    | 验证打包文件的存在性、大小和存活统计         |
| `!!pb database validate blobs`    | 验证数据对象的完整性，包括存储大小和哈希值      |
| `!!pb database validate chunks`   | 验证数据块、数据块组及其关系绑定的完整性       |
| `!!pb database validate files`    | 验证文件对象及其与数据对象、文件集的关联       |
| `!!pb database validate filesets` | 验证文件集对象及其与备份、文件的关联         |
| `!!pb database validate backups`  | 验证备份对象及其与文件集的关联            |
| `!!pb database validate all`      | 执行上述全部验证                   |

示例：

```
!!pb database validate all
```

示例输出：

```
> !!pb database validate all
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有打包文件, 请稍等...
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Pack validation start
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 16 / 16 packs
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Pack validation done: total 16, validated 16, ok 16, bad 0
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO]: [PB] 全部16个打包文件都是健康的
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有数据对象, 请稍等...
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:15:51] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部4325个数据对象都是健康的
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有数据块相关对象 (包括数据块、数据块组、关系绑定), 请稍等...
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Chunk validation start
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Chunk validation done: total 0, validated 0, ok 0, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部0个数据块、0个数据块组、0 + 0 个数据块关系绑定对象都是健康的
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有文件对象 (包括文件、文件夹、符号链接), 请稍等...
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: File validation start
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 9147 / 9147 file objects
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: File validation done: total 9147, validated 9147, ok 9147, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 已验证9147/9147个文件
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部9147个文件都是健康的
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有文件集, 请稍等...
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Fileset validation start
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 26 fileset objects
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 26 / 26 fileset objects
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Fileset validation done: total 26, validated 26, ok 26, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 已验证26/26个文件集
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部26个文件集都是健康的
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有备份, 请稍等...
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Backup validation start
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 21 backup objects
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Backup validation done: total 21, validated 21, ok 21, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 已验证21/21个备份
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部21个备份都是健康的
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 验证完成, 耗时15.32s。打包文件: 健康, 数据对象: 健康, 数据块相关对象: 健康, 文件对象: 健康, 文件集: 健康, 备份: 健康
```

### 验证内容

打包文件：

- 文件存在性：检查打包文件是否存在于存储中
- 大小匹配：检查打包文件大小与数据库记录是否一致
- 存活统计：检查存活大小与存活条目数是否未超过总大小和总条目数

直存数据对象：

- 文件存在性：检查直存数据对象文件是否存在于存储中
- 完整性：验证数据对象文件的哈希值
- 大小匹配：检查存储大小与数据库记录是否一致
- 压缩验证：验证压缩数据能否完整解压

对于分块数据对象，`validate blobs` 会验证其布局和大小统计。
若要检查数据块内容是否完整，还需要执行 `validate chunks`

数据块：

- 打包条目关系：检查每个数据块是否引用存在的打包条目
- 完整性：验证从打包条目读取的数据块内容
- 大小匹配：检查存储大小与数据库记录是否一致
- 孤立检测：检查未被任何数据块组引用的数据块，以及未被任何数据对象引用的数据块组
- 绑定一致性：验证数据块组-数据块绑定与数据对象-数据块组绑定的正确性

文件：

- 引用完整性：检查文件引用的数据对象是否存在
- 文件集关联：验证文件与文件集的关联关系
- 元数据一致性：检查文件中保存的数据对象信息是否与数据对象记录一致

文件集：

- 备份关联：验证文件集与备份的关联关系
- 文件计数：检查文件集中的文件数量
- 大小计算：验证文件集大小统计是否正确

备份：

- 文件集引用：检查备份引用的文件集是否存在
- 文件计数：检查备份文件数量是否与文件集一致
- 大小匹配：检查备份大小是否与文件集统计一致


当验证发现问题时，会显示详细的错误信息：

```
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4322, bad 3
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 发现了3/4325个异常数据对象
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 文件缺失的数据对象: 2个
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 1. 4fc02ad5e7508949634f012fb96c2968: blob file does not exist
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 2. 54695a153daae89e685894ee966a277e: blob file does not exist
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 信息不匹配的数据对象: 1个
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 1. 8b36d71e250e25527f59b8fd9e0f2dce: stored size mismatch, expect 178, found 176
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 影响范围: 6/9147个文件对象, 2/26个文件集, 16/21个备份
[MCDR] [23:19:19] [PB@fc91-worker-heavy/INFO]: [PB] 见日志文件 pb_files/logs/validate.log 以了解这些数据对象的详细信息及影响范围
```


## 数据库清理与整理

### 数据库清理

清理数据库中的孤立对象等无效数据：

```
!!pb database prune
```

清理范围包括：

- 孤立数据对象：不再被任何文件引用的数据对象
- 孤立文件：不再被任何文件集引用的文件
- 孤立文件集：不再被任何备份引用的文件集
- 基础文件集缩减：清理基础文件集中不再被使用的文件条目
- 未知数据对象文件：磁盘上存在但数据库中无记录的数据对象文件
- 打包文件整理：重写存活条目，并回收无效空间过多的打包文件
- 未知打包文件：磁盘上存在但数据库中无记录的打包文件

!!! note

    绝大多数情况下无需手动执行此命令；Prime Backup 在日常操作中会自动保持数据库干净

### 手动打包文件整理

以 100% 存活数据阈值整理所有打包文件：

```
!!pb database compact_packs
```

该命令会重写包含无效空间的打包文件，并删除已无存活条目的打包文件

全部条目均存活的打包文件不会被重写

!!! note

    此命令主要用于手动维护与调试；日常清理仍请使用 `!!pb database prune`

### SQLite 数据库整理

整理 SQLite 数据库文件，回收磁盘空间：

```
!!pb database vacuum
```

示例输出：

```
> !!pb database vacuum
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO]: [PB] 正在整理数据库文件, 请稍等...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO]: [PB] 数据库文件整理完毕, 耗时2.34s, 体积变化: 45.67MiB -> 32.15MiB (-13.52MiB) (70.40%)
```

!!! note

    默认配置下，Prime Backup 会周期性自动执行 SQLite 数据库整理任务，绝大多数情况下无需手动执行此命令
