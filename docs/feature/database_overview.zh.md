---
title: '数据库概览'
---

查看数据库统计信息概览，用于快速了解 Prime Backup 数据库的信息情况

## 命令

```
!!pb database overview
```

## 功能

查看 Prime Backup 数据库的整体统计信息

## 示例输出

```
> !!pb database overview
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] ======== 数据库概览 ========
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据库版本: 4
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据库文件大小: 2.67MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 哈希算法: blake3
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [备份]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 备份数: 22
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 备份存储实际大小总和: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 备份存储未压缩大小总和: 657.79MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 所有备份文件原始大小总和: 2.50GiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 总压缩率（去重 + 压缩）: 17.6%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [文件]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 文件集数: 26
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 文件数: 70469 (9190 个对象)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 文件对象去重统计: 数量 -86.95%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [数据对象]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象数: 4339
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象总储存大小: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象总原始大小: 657.79MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象去重统计: 数量 -52.79%, 大小 -74.31%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [数据块]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据块数: 1280
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据块总储存大小: 190.23MiB (74.1%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据块总原始大小: 256.82MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据块去重统计: 数量 -31.44%, 大小 -57.62%
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] [打包文件]
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 打包文件数量: 16
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 打包文件大小总和: 190.23MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 打包文件存活大小总和: 176.91MiB (93.0%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 打包文件存活条目数量: 1280
```

## 输出分区

- 备份：展示备份总数，以及当前以直存数据对象和打包文件形式保存的数据总量
- 文件：展示逻辑备份内容对应的文件集与文件统计信息
- 数据对象：展示数据对象的统计信息
- 数据块：展示数据块的统计信息
- 打包文件：展示打包文件和存活打包条目的统计信息
