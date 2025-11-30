---
title: '备份还原（回档）'
---

还原一个备份，也回档操作

## 回档步骤

### MCDR 环境

Prime Backup 目前仅支持在 MCDR 环境中进行备份回档

在 MCDR 环境中回档一个备份，只需执行如下指令：

```
!!pb back
```

此命令将回档至最新的非临时备份。如需回档至特定备份，可指定备份 ID：

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

回档命令支持以下可选参数：

| 参数            | 说明              |
|---------------|-----------------|
| `--confirm`   | 跳过确认步骤，直接开始回档   |
| `--fail-soft` | 在导出过程中跳过导出失败的文件 |
| `--no-verify` | 不校验导出文件的内容      |

示例：

```
!!pb back 12 --confirm --fail-soft
```

### 命令行环境

Prime Backup 暂不支持在命令行环境里恢复备份（下次一定！）

## 相关配置

回档相关的配置基本位于下述两个小节：

- [服务器配置](../config.zh.md#服务器配置)，包含在回档时与 MC 服务器的交互命令
- [备份配置](../config.zh.md#备份配置)，包含回档时的文件处理规则

## 回档流程详解

下面将列出 PB 回档备份过程中的操作流程

1. 确认阶段
   1. 显示要回档的备份信息
   2. 等待用户确认（除非使用 `--confirm` 参数）
   3. 用户可通过 `!!pb confirm` 确认或 `!!pb abort` 终止
2. 服务器关闭
   1. 如果服务器正在运行，执行 10s 倒计时（默认配置 `command.restore_countdown_sec: 10`）
   2. 倒计时期间可以取消回档操作
   3. 停止服务器并等待完全关闭
3. 回档前的备份
   1. 如果配置了 `backup_on_restore`（默认值 `true`），将在此时创建一个备份，以便不时之需
   2. 备份注释为"回档至#X前的自动备份"
   3. 此备份将被标记为临时备份，将在备份清理时特殊处理
4. 实际回档操作
   1. 回收站机制：将备份目标目录中现有的所有文件移动到临时回收站，确保回档失败时可完全回滚
   2. 保留文件处理：如果配置了 `retain_patterns`，使用 gitignore 风格模式匹配并隔离要保留的文件
   3. 文件导出：使用多线程并行将备份文件导出到目标目录
   4. 属性恢复：恢复文件权限、时间戳、所有者和符号链接目标
   5. 保留文件恢复：最后将 `retain_patterns` 保留的文件移回原位置
5. 服务器重新启动
   1. 如果服务器原本在运行，重启服务器
