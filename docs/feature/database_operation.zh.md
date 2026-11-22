---
title: '数据库操作'
---

存储算法迁移与备份数据修改操作

## 哈希算法迁移

!!! warning

    - 该操作会影响全部备份数据，建议先做好额外备份
    - 迁移耗时取决于文件数量和磁盘 I/O 速度，且任务开始后不支持中断

将数据库中的哈希算法迁移至新算法：

```
!!pb database migrate_hash_method <新哈希算法>
```

TODO

## 压缩方法迁移

!!! warning

    - 该操作会影响全部备份数据，建议先做好额外备份
    - 迁移耗时取决于文件数量和磁盘 I/O 速度，且任务开始后不支持中断

将已储存备份数据的压缩方法迁移至新方法：

```
!!pb database migrate_compress_method <新压缩方法>
```

TODO

## 文件删除

### 删除备份中的文件

删除备份中的指定文件：

```
!!pb database delete file <备份ID> <文件路径>
```

示例：

```
!!pb database delete file 59 world/cache/file.txt
```

### 递归删除目录

递归删除备份中的指定目录及其全部内容：

```
!!pb database delete file <备份ID> <目录路径> --recursive
```

示例：

```
!!pb database delete file 59 world/cache --recursive
```

注意事项：

- 不能删除备份的根目录
- 删除目录必须传入 `--recursive` 参数
- 默认需要用户确认，以防误操作
