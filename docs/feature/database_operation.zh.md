---
title: '数据库操作'
---

配置迁移和数据修改操作

## 概述

Prime Backup 的数据库操作功能允许你修改备份系统的配置和数据，包括哈希方法迁移、压缩方法迁移和文件删除等操作。这些功能对于系统维护和优化非常重要。

## 哈希方法迁移

### 迁移哈希算法

将数据库中的哈希算法迁移到新的算法：
```
!!pb database migrate_hash_method <新哈希方法>
```

支持的哈希方法：
- `md5` - MD5 (128位)
- `sha1` - SHA-1 (160位)
- `sha256` - SHA-256 (256位)
- `blake2b` - BLAKE2b (256位)
- `blake3` - BLAKE3 (256位)

示例：
```
!!pb database migrate_hash_method sha256
```

示例输出：
```
> !!pb database migrate_hash_method sha256
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 当前哈希方法: md5
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 目标哈希方法: sha256
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 此操作将重新计算所有文件的哈希值，可能需要较长时间
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 请在 60 秒内作出选择并输入对应指令:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认迁移: !!pb confirm
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 终止迁移: !!pb abort
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认迁移
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始迁移哈希方法: md5 -> sha256
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 正在重新计算文件哈希值...
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 迁移完成: md5 -> sha256
```

### 迁移流程

1. **检查当前哈希方法**：显示当前使用的哈希算法
2. **验证新方法**：检查新哈希算法是否可用
3. **用户确认**：需要用户确认迁移操作
4. **重新计算哈希**：重新计算所有文件的哈希值
5. **更新配置**：更新数据库配置

### 注意事项

- **不可逆操作**：哈希方法迁移是不可逆的
- **时间消耗**：可能需要较长时间，取决于文件数量
- **数据完整性**：迁移过程中确保数据完整性
- **权限要求**：需要权限等级 3

## 压缩方法迁移

### 迁移压缩算法

将数据库中的压缩算法迁移到新的算法：
```
!!pb database migrate_compress_method <新压缩方法>
```

支持的压缩方法：
- `none` - 不压缩
- `gzip` - GZIP 压缩
- `bzip2` - BZIP2 压缩
- `lzma` - LZMA 压缩
- `zstd` - Zstandard 压缩

示例：
```
!!pb database migrate_compress_method zstd
```

示例输出：
```
> !!pb database migrate_compress_method zstd
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 将迁移压缩方法至 zstd，压缩阈值 1.00MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 此操作将重新压缩所有文件，可能需要较长时间
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 请在 60 秒内作出选择并输入对应指令:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认迁移: !!pb confirm
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 终止迁移: !!pb abort
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认迁移
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始迁移压缩方法: zstd
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 正在重新压缩文件...
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 迁移完成: 存储大小 118.39MiB -> 102.64MiB (-15.75MiB)
```

### 迁移流程

1. **显示迁移信息**：显示目标压缩方法和压缩阈值
2. **验证新方法**：检查新压缩算法是否可用
3. **用户确认**：需要用户确认迁移操作
4. **重新压缩文件**：重新压缩所有文件
5. **显示结果**：显示存储大小的变化

### 压缩阈值

- 小于压缩阈值的文件不会被压缩
- 默认压缩阈值为 1.00MiB
- 可在配置文件中调整

## 文件删除

### 删除备份中的文件

删除备份中的特定文件：
```
!!pb database delete file <backup_id> <文件路径>
```

示例：
```
!!pb database delete file 59 world/cache/file.txt
```

### 递归删除目录

递归删除备份中的目录及其所有内容：
```
!!pb database delete file <backup_id> <目录路径> --recursive
```

示例：
```
!!pb database delete file 59 world/cache --recursive
```

示例输出：
```
> !!pb database delete file 59 world/cache --recursive
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始删除备份 #59 中的文件 world/cache
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 目录信息: 类型=目录, 文件总数=45, 普通文件=38, 目录=5, 符号链接=2, 原始大小=12.45MiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 请在 60 秒内作出选择并输入对应指令:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认删除: !!pb confirm
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 终止删除: !!pb abort
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认删除
[MCDR] [00:46:12] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 删除完成: 备份 #59, 文件 world/cache, 释放存储 8.21MiB
```

### 跳过确认

跳过确认步骤直接删除：
```
!!pb database delete file <backup_id> <文件路径> --confirm
```

### 删除限制

- **根目录保护**：不能删除备份的根目录
- **目录删除**：删除目录需要 `--recursive` 参数
- **确认机制**：默认需要用户确认，防止误操作

## 权限要求

| 操作 | 权限等级 | 说明 |
|------|----------|------|
| 哈希方法迁移 | 3 | 迁移哈希算法 |
| 压缩方法迁移 | 3 | 迁移压缩算法 |
| 删除文件 | 3 | 删除备份中的文件 |

## 安全机制

### 确认机制

所有修改操作都包含确认机制：
- 默认需要用户确认
- 60秒内可取消操作
- 支持 `--confirm` 参数跳过确认

### 数据完整性

- **原子操作**：迁移操作是原子的，失败时可回滚
- **进度跟踪**：显示操作进度和结果
- **错误处理**：遇到错误时提供详细错误信息

## 实用示例

### 迁移到更安全的哈希算法
```
!!pb database migrate_hash_method sha256
```

### 迁移到更高效的压缩算法
```
!!pb database migrate_compress_method zstd
```

### 删除临时文件
```
!!pb database delete file 59 world/cache --recursive --confirm
```

### 删除特定文件
```
!!pb database delete file 59 world/logs/server.log --confirm
```

## 注意事项

1. **备份建议**：在执行迁移操作前建议创建完整备份
2. **时间预估**：迁移操作可能需要较长时间，取决于数据量
3. **资源消耗**：迁移操作会消耗较多CPU和内存资源
4. **不可逆性**：哈希方法迁移是不可逆的
5. **压缩效率**：不同的压缩算法有不同的压缩效率和速度

通过这些操作功能，你可以优化备份系统的性能，更新安全配置，以及清理不需要的文件。
