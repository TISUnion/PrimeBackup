---
title: '数据库查看'
---

查看和审查数据库中的对象信息

## 概述

PrimeBackup 的数据库查看功能允许您深入查看备份系统中的各个组件，包括备份、文件集、文件和blob。这些功能对于调试、审计和了解备份系统的内部结构非常有用。

## 备份查看

### 查看备份详细信息

查看特定备份的完整信息：
```
!!pb database inspect backup <backup_id>
```

示例：
```
!!pb database inspect backup 59
```

示例输出：
```
> !!pb database inspect backup 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 备份 #59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] ID: 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 时间戳: 1760892371872862 (2024-01-01 12:00:00)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 创建者: "console:"
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 注释: "香猪烤好了"
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 目标: world
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 标签: {"scheduled": true, "protected": false}
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件集: 基础文件集 #123, 增量文件集 #124
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 原始大小: 139.28MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 存储大小: 118.39MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件总数: 4106
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] - 普通文件: 3802
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] - 目录: 254
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] - 符号链接: 50
```

### 查看的信息内容

- **基本信息**: ID、时间戳、创建者、注释
- **配置信息**: 备份目标、标签
- **存储信息**: 文件集ID、原始大小、存储大小
- **文件统计**: 文件总数、按类型分类的文件数量

## 文件查看

### 查看备份中的文件

查看备份中特定文件的详细信息：
```
!!pb database inspect file <backup_id> <文件路径>
```

示例：
```
!!pb database inspect file 59 world/level.dat
```

示例输出：
```
> !!pb database inspect file 59 world/level.dat
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件集 #123 文件 world/level.dat
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件集ID: 123
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 路径: world/level.dat
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 角色: 普通文件
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 权限: 644 (rw-r--r--)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob哈希: 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob压缩: zstd
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob原始大小: 8.21KiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob存储大小: 4.12KiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 用户ID: 1000 (minecraft)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 组ID: 1000 (minecraft)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 修改时间: 1760892371872862 (2024-01-01 12:00:00.728628)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 被使用次数: 3 (#59, #60, #61)
```

### 查看文件集中的文件

查看文件集中特定文件的详细信息：
```
!!pb database inspect file2 <fileset_id> <文件路径>
```

示例：
```
!!pb database inspect file2 123 world/level.dat
```

### 文件信息内容

- **文件元数据**: 路径、角色、权限、用户/组信息
- **存储信息**: Blob哈希、压缩算法、大小信息
- **时间信息**: 修改时间
- **使用情况**: 被哪些备份使用

## 文件集查看

### 查看文件集详细信息

查看特定文件集的完整信息：
```
!!pb database inspect fileset <fileset_id>
```

示例：
```
!!pb database inspect fileset 123
```

示例输出：
```
> !!pb database inspect fileset 123
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件集 #123
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] ID: 123
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 类型: 基础文件集
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件对象数: 4106
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件数: 3802
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 原始大小: 139.28MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 存储大小: 118.39MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 被使用次数: 1 (#59)
```

### 文件集信息内容

- **基本信息**: ID、类型（基础/增量）
- **文件统计**: 文件对象数、文件数
- **大小信息**: 原始大小、存储大小
- **使用情况**: 被哪些备份使用

## Blob查看

### 查看Blob详细信息

查看特定blob的完整信息：
```
!!pb database inspect blob <blob_hash>
```

示例：
```
!!pb database inspect blob 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b
```

示例输出：
```
> !!pb database inspect blob 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob 8a3b9c7d2e1f4a5b
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 哈希: 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 压缩: zstd
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 原始大小: 8.21KiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 存储大小: 4.12KiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 被使用次数: 3
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 1. 文件集 #123, 路径: world/level.dat
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 2. 文件集 #124, 路径: world/level.dat
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 3. 文件集 #125, 路径: world/level.dat
```

### Blob信息内容

- **存储信息**: 哈希值、压缩算法、大小信息
- **使用情况**: 被哪些文件使用（文件集ID和路径）

## 交互功能

### 点击操作

所有查看功能都支持丰富的交互操作：

- **点击备份ID**: 跳转到该备份的查看页面
- **点击文件集ID**: 跳转到该文件集的查看页面
- **点击Blob哈希**: 跳转到该blob的查看页面
- **点击文件路径**: 查看该文件的详细信息

### 悬停提示

- **备份ID**: 悬停显示备份的创建时间和注释
- **文件集ID**: 悬停显示文件集的类型和大小
- **Blob哈希**: 悬停显示完整的哈希值
- **文件路径**: 悬停显示文件的完整路径

## 权限要求

| 操作 | 权限等级 | 说明 |
|------|----------|------|
| 查看备份 | 1 | 查看备份详细信息 |
| 查看文件 | 1 | 查看文件详细信息 |
| 查看文件集 | 1 | 查看文件集详细信息 |
| 查看Blob | 1 | 查看blob详细信息 |

## 实用示例

### 查看最近备份的详细信息
```
!!pb database inspect backup ~
```

### 查看特定文件的存储情况
```
!!pb database inspect file 59 world/level.dat
```

### 查看文件集的使用情况
```
!!pb database inspect fileset 123
```

### 查看blob的共享情况
```
!!pb database inspect blob 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b
```

## 注意事项

1. **数据模型**: PrimeBackup 使用 Backup-Fileset-File-Blob 的四层数据模型
2. **共享机制**: 相同的文件内容会共享同一个blob，减少存储占用
3. **交互性**: 所有查看结果都支持点击交互，便于导航
4. **详细信息**: 提供完整的元数据信息，便于调试和审计

通过这些查看功能，您可以深入了解备份系统的内部结构，检查文件存储情况，以及调试可能出现的问题。
