---
title: '数据库维护'
---

一致性状态审查和数据库整理

## 概述

Prime Backup 的数据库维护功能用于确保备份系统的数据一致性和完整性，包括数据库验证、清理和压缩等操作。这些功能对于长期运行的备份系统至关重要。

## 数据库概览

### 查看数据库统计信息

查看数据库的整体统计信息：
```
!!pb database overview
```

示例输出：
```
> !!pb database overview
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] ======== 数据库概览 ========
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 数据库版本: 3
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 数据库文件大小: 2.67MiB
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 哈希算法: xxh128
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 备份数: 21
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 文件数: 66351 (9147 个对象)
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 文件总大小: 2.37GiB
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 数据对象数: 4325
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 数据对象总储存大小: 428.23MiB (68.3%)
[MCDR] [23:21:50] [PB@fc91-worker-light/INFO]: [PB] 数据对象总原始大小: 627.04MiB
```


## 数据库验证

### 完整验证

验证数据库所有组件的完整性：

```
!!pb database validate all
```

### 部分验证

验证特定组件的完整性：

- `!!pb database validate blobs` - 验证数据对象
- `!!pb database validate files` - 验证文件记录
- `!!pb database validate filesets` - 验证文件集
- `!!pb database validate backups` - 验证备份记录

示例：

```
!!pb database validate all
```

示例输出：
```
> !!pb database validate all
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO]: [PB] 正在验证所有数据对象, 请稍等...
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation start
[MCDR] [23:15:38] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 3000 / 4325 blobs
[MCDR] [23:15:51] [PB@fc91-worker-heavy/INFO] [prime_backup]: Validating 4325 / 4325 blobs
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO] [prime_backup]: Blob validation done: total 4325, validated 4325, ok 4325, bad 0
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 已验证4325/4325个数据对象
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 全部4325个数据对象都是健康的
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
[MCDR] [23:15:53] [PB@fc91-worker-heavy/INFO]: [PB] 验证完成, 耗时15.32s。数据对象: 健康, 文件对象: 健康, 文件集: 健康, 备份: 健康
```

### 验证内容

数据对象：

- 文件存在性: 检查数据对象文件是否存在于存储中
- 完整性: 验证数据对象文件的哈希值
- 大小匹配: 检查存储大小与记录是否一致
- 压缩验证: 验证压缩数据的完整性

文件：

- 引用完整性: 检查文件引用的数据对象是否存在
- 文件集关联: 验证文件与文件集的关联关系
- 元数据一致性: 检查文件元数据是否完整

文件集：

- 备份关联: 验证文件集与备份的关联关系
- 文件计数: 检查文件集中的文件数量
- 大小计算: 验证文件集大小计算的正确性

备份：

- 文件集引用: 检查备份引用的文件集是否存在
- 时间戳顺序: 验证备份时间戳的顺序
- 元数据完整性: 检查备份元数据是否完整


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


## 数据库清理

### 清理孤立对象

清理数据库中的孤立对象（不再被引用的对象）

```
!!pb database prune
```

清理内容

- 孤立数据对象: 不再被任何文件引用的数据对象
- 孤立文件: 不再被任何文件集引用的文件
- 孤立文件集: 不再被任何备份引用的文件集
- 基础文件集压缩: 优化基础文件集的存储结构
- 未知数据对象文件: 存储中存在但数据库中无记录的数据对象文件

!!! note

    绝大部分情况下，你都无需手动执行此命令。Prime Backup 在日常操作中是会确保数据库处于干净状态


## SQLite压缩

### SQLite 压缩

压缩 SQLite 数据库文件，减少磁盘占用

```
!!pb database vacuum
```

示例输出：
```
> !!pb database vacuum
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始压缩数据库...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 压缩完成: 耗时 2.34s, 大小 45.67MiB -> 32.15MiB (-13.52MiB, 70.4%)
```

!!! note

    在默认配置下，Prime Backup 会自动周期性执行 SQLite 数据库任务，因此大部分情况下，你都无需手动执行此命令 
