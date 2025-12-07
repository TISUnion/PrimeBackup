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
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据库版本: 3
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据库文件大小: 2.67MiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 哈希算法: xxh128
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 备份数: 22
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 文件数: 70469 (9190 个对象)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 文件总大小: 2.50GiB
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象数: 4339
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象总储存大小: 451.68MiB (68.7%)
[MCDR] [02:41:29] [PB@c8df-worker-light/INFO]: [PB] 数据对象总原始大小: 657.79MiB
```
