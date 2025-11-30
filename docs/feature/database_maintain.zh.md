---
title: '数据库维护'
---

一致性状态审查和数据库整理

## 概述

PrimeBackup 的数据库维护功能用于确保备份系统的数据一致性和完整性，包括数据库验证、清理和压缩等操作。这些功能对于长期运行的备份系统至关重要。

## 数据库验证

### 完整验证

验证数据库所有组件的完整性：
```
!!pb database validate all
```

### 部分验证

验证特定组件的完整性：
- `!!pb database validate blobs` - 验证blob文件
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
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始验证数据库...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证blobs...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证blobs完成: 已验证 1250 / 1250
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 所有blob验证通过 (1250)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证files...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证files完成: 已验证 41060 / 41060
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 所有文件验证通过 (41060)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证filesets...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证filesets完成: 已验证 59 / 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 所有文件集验证通过 (59)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证backups...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证backups完成: 已验证 59 / 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 所有备份验证通过 (59)
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证完成: 耗时 12.45s, blobs: 通过, files: 通过, filesets: 通过, backups: 通过
```

### 验证内容

#### Blob验证
- **文件存在性**: 检查blob文件是否存在于存储中
- **完整性**: 验证blob文件的哈希值
- **大小匹配**: 检查存储大小与记录是否一致
- **压缩验证**: 验证压缩数据的完整性

#### 文件验证
- **引用完整性**: 检查文件引用的blob是否存在
- **文件集关联**: 验证文件与文件集的关联关系
- **元数据一致性**: 检查文件元数据是否完整

#### 文件集验证
- **备份关联**: 验证文件集与备份的关联关系
- **文件计数**: 检查文件集中的文件数量
- **大小计算**: 验证文件集大小计算的正确性

#### 备份验证
- **文件集引用**: 检查备份引用的文件集是否存在
- **时间戳顺序**: 验证备份时间戳的顺序
- **元数据完整性**: 检查备份元数据是否完整

### 错误处理

当验证发现问题时，会显示详细的错误信息：
```
> !!pb database validate blobs
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证blobs...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 验证blobs完成: 已验证 1248 / 1250
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 发现损坏的blob: 2 / 1248
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 损坏的blob: 2
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 1. 8a3b9c7d2e1f4a5b6c7d8e9f0a1b2c3d4e5f6a7b: 文件不存在
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 2. 9b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c: 哈希值不匹配
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 受影响文件数 / 总文件数: 15 / 41060
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 受影响文件集数 / 总文件集数: 3 / 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 受影响备份数 / 总备份数: 5 / 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 查看详细日志: /path/to/validate.log
```

## 数据库清理

### 清理孤立对象

清理数据库中的孤立对象（不再被引用的对象）：
```
!!pb database prune
```

示例输出：
```
> !!pb database prune
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始清理数据库...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 清理完成: 孤立blob 5, 孤立文件 12, 孤立文件集 2, 压缩基础文件集 3, 未知blob文件 2
```

### 清理内容

#### 孤立对象清理
- **孤立blob**: 不再被任何文件引用的blob
- **孤立文件**: 不再被任何文件集引用的文件
- **孤立文件集**: 不再被任何备份引用的文件集

#### 文件集压缩
- **基础文件集压缩**: 优化基础文件集的存储结构
- **增量合并**: 将增量文件集合并到基础文件集

#### 未知文件清理
- **未知blob文件**: 存储中存在但数据库中无记录的blob文件

### 清理效果

清理操作可以：
- 释放存储空间
- 提高数据库性能
- 减少数据冗余
- 优化查询效率

## 数据库压缩

### SQLite数据库压缩

压缩SQLite数据库文件，减少磁盘占用：
```
!!pb database vacuum
```

示例输出：
```
> !!pb database vacuum
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始压缩数据库...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 压缩完成: 耗时 2.34s, 大小 45.67MiB -> 32.15MiB (-13.52MiB, 70.4%)
```

### 压缩效果

- **空间回收**: 回收数据库文件中未使用的空间
- **性能优化**: 提高数据库读写性能
- **碎片整理**: 整理数据库文件碎片

## 数据库概览

### 查看数据库统计信息

查看数据库的整体统计信息：
```
!!pb database overview
```

示例输出：
```
> !!pb database overview
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 数据库概览
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 备份数: 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件集数: 118
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 文件数: 41060
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] Blob数: 1250
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 总原始大小: 8.21GiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 总存储大小: 6.45GiB
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 压缩率: 78.5%
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 数据库文件大小: 32.15MiB
```

## 权限要求

| 操作 | 权限等级 | 说明 |
|------|----------|------|
| 数据库验证 | 1 | 验证数据库完整性 |
| 数据库清理 | 3 | 清理孤立对象 |
| 数据库压缩 | 3 | 压缩数据库文件 |
| 数据库概览 | 1 | 查看数据库统计 |

## 维护计划

### 定期维护建议

- **每日**: 运行 `!!pb database overview` 检查系统状态
- **每周**: 运行 `!!pb database validate blobs` 验证数据完整性
- **每月**: 运行 `!!pb database prune` 清理孤立对象
- **每季度**: 运行 `!!pb database vacuum` 压缩数据库

### 故障排查

当系统出现问题时：
1. 运行 `!!pb database validate all` 进行全面验证
2. 检查验证日志中的错误信息
3. 根据错误类型采取相应措施
4. 必要时运行 `!!pb database prune` 进行修复

## 实用示例

### 定期健康检查
```
!!pb database validate all
```

### 清理存储空间
```
!!pb database prune
```

### 优化数据库性能
```
!!pb database vacuum
```

### 查看系统状态
```
!!pb database overview
```

## 注意事项

1. **验证时间**: 完整验证可能需要较长时间，取决于数据量
2. **清理风险**: 清理操作会删除数据，建议先验证
3. **压缩时机**: 在大量删除操作后运行压缩效果更好
4. **备份建议**: 在执行维护操作前建议创建完整备份
5. **监控指标**: 关注存储大小、文件数量和数据库性能

通过这些维护功能，您可以确保备份系统的长期稳定运行，及时发现和修复问题，优化系统性能。
