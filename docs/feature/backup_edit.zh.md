---
title: '备份编辑'
---

编辑和管理备份

## 备份重命名

### 修改备份注释

修改备份的注释信息：

```
!!pb rename <backup_id> <新注释>
```

示例：
```
!!pb rename 59 香猪烤好了
```

示例输出：
```
> !!pb rename 59 香猪烤好了
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 备份 #59 注释已更新为: 香猪烤好了
```

### 功能说明

- **备份 ID**：支持多种格式（正整数、`~`、相对偏移等）
- **新注释**：可以是任意文本，支持空格和特殊字符
- **权限要求**：权限等级 2

## 备份标签管理

### 查看备份标签

查看备份的所有标签：
```
!!pb tag <backup_id>
```

查看特定标签的值：
```
!!pb tag <backup_id> <标签名>
```

示例：
```
!!pb tag 59
!!pb tag 59 protected
```

示例输出：
```
> !!pb tag 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 备份 #59 标签:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB]   scheduled: true
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB]   protected: false
```

### 设置标签值

设置备份标签的值：
```
!!pb tag <backup_id> <标签名> set <值>
```

示例：
```
!!pb tag 59 protected set true
!!pb tag 59 temporary set false
```

### 清除标签

清除备份的标签：
```
!!pb tag <backup_id> <标签名> clear
```

示例：
```
!!pb tag 59 protected clear
```

### 支持的标签类型

Prime Backup 支持以下标签类型：

| 标签名 | 类型 | 说明 | 默认值 |
|-------|------|------|--------|
| `hidden` | 布尔值 | 隐藏备份，在普通列表中不显示 | `false` |
| `temporary` | 布尔值 | 临时备份，在清理时优先删除 | `false` |
| `protected` | 布尔值 | 受保护备份，不能被删除 | `false` |
| `scheduled` | 布尔值 | 计划备份，由定时任务创建 | `false` |

### 标签功能说明

- **隐藏备份**：标记为 `hidden` 的备份在普通 `!!pb list` 中不显示，需要使用 `--all` 参数查看
- **临时备份**：标记为 `temporary` 的备份在清理时会被优先删除
- **受保护备份**：标记为 `protected` 的备份不能被删除操作删除，防止误删重要备份
- **计划备份**：由定时任务创建的备份会自动标记为 `scheduled`

## 备份删除

### 删除单个备份

删除指定的备份：
```
!!pb delete <backup_id>
```

示例：
```
!!pb delete 59
```

### 删除多个备份

删除多个备份（支持备份ID列表）：
```
!!pb delete <backup_id1> <backup_id2> <backup_id3>
```

示例：
```
!!pb delete 59 60 61
```

### 删除备份范围

删除指定范围内的所有备份：
```
!!pb delete_range <起始ID>-<结束ID>
```

示例：
```
!!pb delete_range 50-60
```

### 确认机制

删除操作默认需要用户确认：
```
> !!pb delete 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 将要删除备份 #59: 2025-01-02 18:00:00, 注释: 香猪烤好了
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 请在 60 秒内作出选择并输入对应指令:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认删除: !!pb confirm
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 终止删除: !!pb abort
```

跳过确认：
```
!!pb delete 59 --confirm
```

### 保护机制

受保护的备份不能被删除：
```
> !!pb delete 59
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 备份 #59 受保护，无法删除
```

## 备份清理

### 智能清理

清理不需要的备份，释放存储空间：
```
!!pb prune
```

示例输出：
```
> !!pb prune
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 开始清理备份...
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 将删除以下备份:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB]   #45: 2024-01-01 10:00:00, 注释: 临时备份
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB]   #46: 2024-01-01 10:30:00, 注释: 临时备份
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 请在 60 秒内作出选择并输入对应指令:
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 确认清理: !!pb confirm
[MCDR] [00:46:11] [PB@51f5-worker-heavy/INFO] [prime_backup]: [PB] 终止清理: !!pb abort
```

### 清理策略

Prime Backup 的清理功能基于以下策略：

1. **临时备份优先**：标记为 `temporary` 的备份会被优先清理
2. **时间策略**：基于配置的保留时间策略选择要删除的备份
3. **保护机制**：标记为 `protected` 的备份不会被清理
4. **安全确认**：清理操作需要用户确认

## 权限要求

| 操作 | 权限等级 | 说明 |
|------|----------|------|
| 重命名备份 | 2 | 修改备份注释 |
| 查看标签 | 1 | 查看备份标签信息 |
| 设置标签 | 3 | 设置或清除备份标签 |
| 删除备份 | 2 | 删除单个或多个备份 |
| 删除范围 | 3 | 删除备份范围 |
| 清理备份 | 3 | 智能清理备份 |

## 实用示例

### 重命名最近的备份
```
!!pb rename ~ 重要更新备份
```

### 保护重要备份
```
!!pb tag 59 protected set true
```

### 标记临时备份
```
!!pb tag 60 temporary set true
```

### 删除多个临时备份
```
!!pb delete 45 46 47 --confirm
```

### 清理旧备份
```
!!pb prune --confirm
```

## 注意事项

1. **保护机制**：受保护的备份不能被删除或清理
2. **确认机制**：删除和清理操作默认需要用户确认，防止误操作
3. **权限控制**：不同操作有不同的权限等级要求
4. **数据安全**：删除操作会清理相关的文件集和孤立blob，确保数据一致性
5. **级联删除**：删除备份时会自动清理相关的文件集和孤立blob

通过这些编辑功能，你可以灵活地管理备份，包括重命名、标签管理、删除和清理，确保备份系统的有效性和安全性。
