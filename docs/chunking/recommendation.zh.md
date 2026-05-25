---
title: '分块规则推荐'
---

本页列出 Minecraft 存档场景下较常见的分块规则选择建议

这些规则并不都适合放进默认配置。默认配置应保持保守，
只覆盖收益稳定、依赖少、误判概率低的文件类型

## 推荐表

| 文件类型               | patterns                                             | 推荐算法          | 阈值        | 推荐度   |
|--------------------|------------------------------------------------------|---------------|-----------|-------|
| Minecraft Anvil 文件 | `*.mca`                                              | `fixed_auto`  | `256 KiB` | ★★★★★ |
| 日志文件               | `*.log`                                              | `fixed_128k`  | `10 MiB`  | ★★★★★ |
| SQLite 主数据库        | `*.db`, `*.sqlite`, `*.sqlite3`                      | `fastcdc_32k` | `20 MiB`  | ★★★☆☆ |
| SQLite WAL 文件      | `*.db-wal`, `*.sqlite-wal`, `*.sqlite3-wal`, `*.wal` | `fixed_128k`  | `10 MiB`  | ★★★☆☆ |
| JSONL 记录文件         | `*.jsonl`                                            | `fixed_128k`  | `10 MiB`  | ★★★☆☆ |
| JSON / YAML 状态文件   | `*.json`, `*.yaml`, `*.yml`                          | `fastcdc_32k` | `20 MiB`  | ★★☆☆☆ |

## Minecraft Anvil 文件

Minecraft 世界中的 region、entities、poi 等数据通常会储存在 `.mca` 文件中

这类文件内部以 4 KiB 页为基本组织单位。世界运行时，
只有一部分区块、实体或 POI 数据会发生变化，因此旧备份中的大量页面有机会被复用

推荐使用 `fixed_auto`。它以 128 KiB 作为默认粒度，在检测到变化窗口后再退到 4 KiB 粒度，
能在元数据开销和复用效果之间取得比较好的平衡

示例配置：

```json
{
    "algorithm": "fixed_auto",
    "file_size_threshold": 262144,
    "patterns": [
        "*.mca"
    ]
}
```

## SQLite 主数据库

不少插件会使用 SQLite 储存权限、经济、记录、地图缓存或其他结构化数据

主数据库文件常见后缀包括 `.db`、`.sqlite` 和 `.sqlite3`。
这类文件通常不是单纯追加写，数据库页可能在文件中间被修改、移动或重写

推荐使用 `fastcdc_32k`。CDC 根据内容决定分块边界，比固定大小分块更能适应数据库内部局部变化

这类规则不建议放入默认配置，因为它会引入 `pyfastcdc` 可选依赖，
并且不是所有 `.db` 文件都足够大或具备稳定的复用收益

示例配置：

```json
{
    "algorithm": "fastcdc_32k",
    "file_size_threshold": 20971520,
    "patterns": [
        "*.db",
        "*.sqlite",
        "*.sqlite3"
    ]
}
```

## SQLite WAL 文件

SQLite 在 WAL 模式下会将新写入先记录到 WAL 文件中，常见后缀包括 `.db-wal`、`.sqlite-wal`、`.sqlite3-wal` 和 `.wal`

WAL 文件的写入模式通常接近尾部追加，因此固定大小分块可以稳定复用前面的旧内容，并避免 CDC 依赖

推荐使用 `fixed_128k`。相比 4 KiB 或 32 KiB 粒度，它能显著减少数据块数量，对大型 WAL 文件更稳妥

需要注意的是，WAL 文件可能在 checkpoint 后被截断或重建。如果它在备份之间经常整体重置，分块收益会下降

示例配置：

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.db-wal",
        "*.sqlite-wal",
        "*.sqlite3-wal",
        "*.wal"
    ]
}
```

## 日志文件

日志文件通常以 `.log` 结尾，常见于服务端本体、MCDR、插件或模组的运行日志

大日志的典型写入模式是尾部追加。对于这种文件，
固定大小分块的边界不会因为追加内容而影响旧数据块，已有内容可以持续复用

推荐使用 `fixed_128k`。它不需要 CDC 依赖，元数据开销也比更小的固定块低很多

示例配置：

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.log"
    ]
}
```

## JSONL 记录文件

部分插件或辅助工具会使用 JSONL 记录事件、统计或历史数据，每一行通常是一条独立记录

如果文件主要以追加记录的方式增长，它和日志文件类似，适合使用固定大小分块

推荐使用 `fixed_128k`。它能稳定复用旧内容，并避免对 CDC 可选依赖的要求

示例配置：

```json
{
    "algorithm": "fixed_128k",
    "file_size_threshold": 10485760,
    "patterns": [
        "*.jsonl"
    ]
}
```

## JSON / YAML 状态文件

`.json`、`.yaml` 和 `.yml` 文件在插件生态中很常见，但大多数配置文件都很小，不值得分块

只有当这类文件变成大型状态文件时，才值得考虑分块。
例如某些插件将玩家数据、领地数据、统计数据或缓存数据写入单个大型文本文件

推荐使用 `fastcdc_32k`。JSON / YAML 被重写时，插入、删除或字段变化可能导致后续内容偏移，
CDC 比固定大小分块更能保留可复用片段

这类规则应谨慎启用。如果文件每次保存都会重新排序字段、刷新大量时间戳或整体改写，分块收益通常不稳定

示例配置：

```json
{
    "algorithm": "fastcdc_32k",
    "file_size_threshold": 20971520,
    "patterns": [
        "*.json",
        "*.yaml",
        "*.yml"
    ]
}
```

## 不推荐的对象

下列文件通常不适合作为分块规则目标：

- 已压缩文件，例如 `.gz`、`.zip`、`.zst`
- 归档或程序包，例如 `.jar`
- 图片、地图瓦片或其他媒体文件
- 大量很小的配置文件
- 每次保存都会整体随机重写的文件

这些文件即使被分块，也通常只能增加哈希、数据库记录和打包条目的开销
