---
title: '数据库操作'
---

配置迁移和数据修改操作

## 哈希算法迁移

!!! warning

    - 影响全部备份内容，建议在做好额外的数据备份
    - 迁移耗时可能会比较长，取决于文件数量、磁盘 IO 速度。迁移任务不支持中断

将数据库中的哈希算法迁移到新的算法：

```
!!pb database migrate_hash_method <新哈希算法>
```

TODO

## 压缩方法迁移

!!! warning

    - 影响全部备份内容，请做好额外的数据备份
    - 迁移耗时可能会比较长，取决于文件数量、磁盘 IO 速度。迁移任务不支持中断

```
!!pb database migrate_compress_method <新压缩方法>
```

TODO

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

注意事项：

- 不能删除备份的根目录
- 删除目录需要 `--recursive` 参数
- 默认需要用户确认，防止误操作
