---
title: '备份回档'
---

将存档回档至指定备份

## 回档步骤

### MCDR 环境

在 MCDR 环境中执行回档，运行以下命令即可：

```
!!pb back
```

该命令会回档至最新的非临时备份。如需回档至指定备份，可传入备份 ID：

```
!!pb back 78
```

示例控制台输出：

```
> !!pb back 78
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] 将要回档至备份#78: 测试备份
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] 请在1分钟内作出选择并输入对应指令:
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] - 确认回档√: !!pb confirm
[MCDR] [22:08:05] [PB@ecac-worker-heavy/INFO]: [PB] - 终止回档×: !!pb abort
> !!pb confirm
[MCDR] [22:08:08] [TaskExecutor/INFO]: [PB] 正在确认回档任务
[MCDR] [22:08:08] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 10秒后将回档至备份#78: 测试备份
[MCDR] [22:08:09] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 9秒后将回档至备份#78: 测试备份
[MCDR] [22:08:10] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 8秒后将回档至备份#78: 测试备份
[MCDR] [22:08:11] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 7秒后将回档至备份#78: 测试备份
[MCDR] [22:08:12] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 6秒后将回档至备份#78: 测试备份
[MCDR] [22:08:13] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 5秒后将回档至备份#78: 测试备份
[MCDR] [22:08:14] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 4秒后将回档至备份#78: 测试备份
[MCDR] [22:08:15] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 3秒后将回档至备份#78: 测试备份
[MCDR] [22:08:16] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 2秒后将回档至备份#78: 测试备份
[MCDR] [22:08:17] [PB@ecac-worker-heavy/INFO] [prime_backup]: [PB] !!! 1秒后将回档至备份#78: 测试备份
[MCDR] [22:08:18] [PB@ecac-worker-heavy/INFO] [prime_backup]: Wait for server to stop
[Server] [22:08:18] [Server thread/INFO]: Stopping the server
[Server] [22:08:18] [Server thread/INFO]: Stopping server
[Server] [22:08:18] [Server thread/INFO]: Saving players
[Server] [22:08:18] [Server thread/INFO]: Saving worlds
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:overworld
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_nether
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: Saving chunks for level 'world'/minecraft:the_end
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (world): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM-1): All chunks are saved
[Server] [22:08:18] [Server thread/INFO]: ThreadedAnvilChunkStorage (DIM1): All chunks are saved
[MCDR] [22:08:19] [MainThread/INFO]: 服务器进程已停止，返回代码为 0
[MCDR] [22:08:19] [MainThread/INFO]: 服务端已关闭
[MCDR] [22:08:19] [PB@ecac-worker-heavy/INFO] [prime_backup]: Creating backup of existing files to avoid idiot
[MCDR] [22:08:19] [PB@ecac-worker-heavy/INFO] [prime_backup]: Scanning file for backup creation at path 'server', targets: ['world']
[MCDR] [22:08:20] [PB@ecac-worker-heavy/INFO] [prime_backup]: Creating backup for ['world'] at path 'server', file cnt 4118, timestamp 1764526100048797, creator 'prime_backup:pre_restore', comment '__pb_translated__:pre_restore:78', tags {'temporary': True}
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Create backup #79 done, +6 blobs (size 6.43MiB / 7.28MiB)
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restoring to backup #78 (fail_soft=False, verify_blob=True)
[MCDR] [22:08:22] [PB@ecac-worker-heavy/INFO] [prime_backup]: Exporting Backup(id=78, timestamp=1763890381206484, creator='player:Fallen_Breath', comment='测试备份', targets=['world'], tags={}, fileset_id_base=56, fileset_id_delta=88, file_count=4118, file_raw_size_sum=136177532, file_stored_size_sum=78537902) to directory server
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO] [prime_backup]: Export done
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO] [prime_backup]: Restore to backup #78 done, cost 16.95s (backup 2.67s, restore 14.28s), starting the server
[MCDR] [22:08:36] [PB@ecac-worker-heavy/INFO]: 正在启动服务端，启动参数为 'java -Xms1G -Xmx2G -jar server.jar'
```

示例游戏内输出：

![pb back](img/pb_back.zh.png)

回档命令支持多种备份 ID 格式：

| 格式             | 示例             | 说明            |
|----------------|----------------|---------------|
| 正整数            | `!!pb back 12` | 回档至指定 ID 的备份  |
| `~` 或 `latest` | `!!pb back ~`  | 回档至最新的非临时备份   |
| 相对偏移           | `!!pb back ~1` | 回档至最新备份的前一个备份 |
| 相对偏移           | `!!pb back ~3` | 回档至最新备份的前三个备份 |

可选参数：

| 参数            | 说明              |
|---------------|-----------------|
| `--confirm`   | 跳过确认步骤，直接开始回档   |
| `--fail-soft` | 导出过程中跳过失败的文件    |
| `--no-verify` | 不校验导出文件的内容完整性   |

示例：

```
!!pb back 12 --confirm --fail-soft
```

### 命令行环境

Prime Backup 也支持在命令行环境中执行核心的回档操作：

```
python3 PrimeBackup.pyz -d pb_files -c config/prime_backup/config.json back -s ./server 78 --confirm
```

命令行 `back` 命令不会停止或启动 Minecraft 服务端，请在运行前自行停止服务端。
若配置文件启用了 `restore.create_pre_restore_backup`，回档前会为 `-s/--source-root` 指定目录中的现有内容创建临时备份。如果该目录不存在，则跳过回档前备份。
可通过 `--no-pre-restore-backup` 参数跳过该临时备份

## 相关配置

回档相关的配置主要位于以下三节：

- [服务器配置](../config.zh.md#服务器配置)：回档时与 Minecraft 服务端交互的相关配置
- [备份配置](../config.zh.md#备份配置)：回档时的文件过滤与处理规则
- [回档配置](../config.zh.md#回档配置)：回档目标目录与回档策略配置

## 回档流程详解

Prime Backup 执行回档的完整流程如下：

1. 确认阶段
   1. 展示目标备份信息
   2. 等待用户确认（使用 `--confirm` 参数时跳过）
   3. 通过 `!!pb confirm` 确认或 `!!pb abort` 终止
2. 服务端关闭
   1. 若服务端正在运行，执行倒计时（默认 `restore.countdown_sec: 10` 秒）
   2. 倒计时期间仍可取消回档
   3. 停止服务端并等待其完全退出
3. 回档前备份
   1. 若启用了 `restore.create_pre_restore_backup`（默认 `true`）且目标目录存在，则为其现有内容创建临时备份。备份来源为 `restore.destination_root`；若为 `null` 则使用 `backup.source_root`
   2. 注释为“回档至#X前的自动备份”
   3. 该备份会带有临时标签，在备份清理时按临时备份规则处理
4. 文件回档
   1. 回收站机制：将目标目录中的现有文件移入临时回收站，确保回档失败时可完整回滚
   2. 保留文件隔离：若配置了 `retain_patterns`，按 gitignore 风格匹配并暂存需要保留的文件
   3. 并行导出：使用多线程将备份文件导出到目标目录；若启用了 `restore.reuse_unchanged_files`，内容未变化的文件会直接从回收站移回
   4. 元数据恢复：恢复文件权限、时间戳、所有者和符号链接目标
   5. 保留文件归位：将 `retain_patterns` 匹配的文件移回原位置
5. 服务端重启
   1. 若回档前服务端处于运行状态，在此重新启动
