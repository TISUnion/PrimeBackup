---
title: '数据库内部对象审查'
---

查看和审查数据库中的对象信息

## 概述

Prime Backup 的数据库查看功能允许你深入查看备份系统中的各个组件，包括备份、文件集、文件和数据对象

## 备份查看

### 查看备份详细信息

查看特定备份的完整信息：

```
!!pb database inspect backup <backup_id>
```

示例：

```
!!pb database inspect backup 45
```

示例输出：

```
> !!pb database inspect backup 45
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] ======== 备份#45 ========
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] ID: 45
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 时间戳 (微秒): 1756050607173147 (日期: 2025-08-24 23:50:07)
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 创建者: "console:" (控制台)
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 注释: "2"
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 备份目标: world
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 标签: {}
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 基础文件集: 39, 增量文件集: 54
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 原始大小: 124137302 (118.39MiB)
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 储存大小: 67760442 (64.62MiB)
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] 文件总数: 4106
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] - 普通文件: 4068
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] - 文件夹: 38
[MCDR] [01:40:29] [PB@f133-worker-light/INFO]: [PB] - 符号链接: 0
```

## 文件查看

### 查看备份中的文件

查看备份中特定文件的详细信息：

```
!!pb database inspect file <backup_id> <文件路径>
```

示例：
```
!!pb database inspect file 45 world/level.dat
```

示例输出：
```
> !!pb database inspect file 45 world/level.dat
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] ======== 文件集39的文件level.dat ========
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 所属文件集: 39
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 路径: world/level.dat
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 角色: 独立
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 模式: 33206 (-rw-rw-rw-)
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 数据对象哈希: 86c7b6e480e869effd9200abe045b3db
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 数据对象压缩方法: zstd
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 数据对象原始大小: 1528 (1.49KiB)
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 数据对象储存大小: 1537 (1.50KiB)
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] Uid: 0
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] Gid: 0
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 修改时间: 1731254748636839 (2024-11-11 00:05:48.636839)
[MCDR] [01:40:53] [PB@f133-worker-light/INFO]: [PB] 包含此文件的备份数量: 3 (样本: #45, #43, #42)
```

### 查看文件集中的文件

查看文件集中特定文件的详细信息：

```
!!pb database inspect file2 <fileset_id> <文件路径>
```

展示的内容同上

## 文件集查看

### 查看文件集详细信息

查看特定文件集的完整信息：

```
!!pb database inspect fileset <fileset_id>
```

示例：

```
!!pb database inspect fileset 87
```

示例输出：

```
> !!pb database inspect fileset 87
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] ======== 文件集87 ========
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] ID: 87
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 类型: 增量文件集
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 文件对象数: 43
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 文件数 (增量): 22
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 原始大小 (增量): 23620147 (22.53MiB)
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 储存大小 (增量): 19735961 (18.82MiB)
[MCDR] [19:37:49] [PB@fc91-worker-light/INFO]: [PB] 关联备份数: 1 (样本: #77)
```

## 数据对象查看

### 查看数据对象详细信息

查看特定数据对象的完整信息：

```
!!pb database inspect blob <哈希值>
```

参数 `<哈希值>` 可以是完整哈希字符串的一个前缀，只要保证唯一即可

示例：

```
!!pb database inspect blob 8a3d
!!pb database inspect blob 8a3d32b705a1274850798ae26ff56ba9
```

示例输出：

```
> !!pb database inspect blob 8a3d
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] ======== 数据对象8a3d32b705a12748 ========
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 哈希: 8a3d32b705a1274850798ae26ff56ba9
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 压缩方法: zstd
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 原始大小: 736 (736.00B)
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 储存大小: 745 (745.00B)
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 关联文件数: 2。样本：
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 1. 文件集39, 路径: world/playerdata/6ccb4484-ca33-41ee-a809-fece13a26c21.dat
[MCDR] [19:38:37] [PB@fc91-worker-light/INFO]: [PB] 2. 文件集56, 路径: world/playerdata/6ccb4484-ca33-41ee-a809-fece13a26c21.dat
```

若提供的哈希值过短，导致无法唯一定位一个数据对象，则会输出：

```
> !!pb database inspect blob 8a
[MCDR] [19:39:53] [PB@fc91-worker-light/INFO]: [PB] 给定的哈希值8a无法唯一确定一个数据对象
[MCDR] [19:39:53] [PB@fc91-worker-light/INFO]: [PB] 找到了至少3个可能的数据对象: 8a92569c48895c60bf2977f1be9591f4, 8abd332ba5b579ca00d0d54e50b8bedd, 8ad9bfd69fa3e84b15e2aa803cac0e4c
```