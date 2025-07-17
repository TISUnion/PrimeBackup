---
title: '配置文件'
---

## 配置文件详解

这是对配置文件的完整解释。如果你想深入了解它，那就接着往下看吧

### 根配置

这是配置文件中的根 json 对象

```json
{
    "enabled": true,
    "debug": false,
    "storage_root": "./pb_files",
    "concurrency": 1,
    
    // 子配置。详见以下各节
    "command": {/* 命令配置 */},
    "server": {/* 服务器配置 */},
    "backup": {/* 备份配置 */},
    "scheduled_backup": {/* 定时备份配置 */},
    "prune": {/* 修剪配置 */},
    "database": {/* 数据库配置 */}
}
```

#### enabled

插件的开关。设置为 `false` 以禁用插件

- 类型：`bool`
- 默认值：`false`

#### debug

调试开关。设置为 `true` 以启用调试日志

- 类型：`bool`
- 默认值：`false`

#### storage_root

Prime Backup 储存各种数据文件所用的根目录

这是一个相对于 MCDR 工作目录的相对路径。默认情况下，根目录将是 `/path/to/mcdr_root/pb_files`

- 类型：`str`
- 默认值：`"./pb_files"`

#### concurrency

在任何任务 / 操作的执行期间使用的最大并发数

将并发数设置为更高的值（例如 `4`）可以加快备份创建和回档等操作的速度，
但这也会让这些操作消耗更多的 CPU

值 `0` 表示使用 50% 的 CPU

- 类型：`int`
- 默认值：`1`

---

### 命令配置

```json
{
    "prefix": "!!pb",
    "permission": {
        "root": 0,
        "abort": 1,  
        "back": 2,
        "confirm": 1,
        "database": 4,
        "delete": 2,
        "delete_range": 3,
        "export": 4,
        "list": 1,
        "make": 1,
        "prune": 3,
        "rename": 2,
        "show": 1,
        "tag": 3
    },
    "confirm_time_wait": "1m",
    "backup_on_restore": true,
    "restore_countdown_sec": 10
}
```

#### prefix

MCDR 中，Prime Backup 所有命令的前缀。通常你不需要更改它

- 类型：`str`
- 默认值：`"!!pb"`

#### permission

所有子命令所需的 [MCDR 权限等级](https://mcdreforged.readthedocs.io/en/latest/permission.html) 的最低要求

它是一个从字符串映射到整数的映射表，存储所有子命令的权限级别要求。
如果子命令不在映射表中，将使用 `1` 作为默认的权限等级要求

例如，在默认配置中，`"back"` 被映射到 `2`，
这意味着 `!!pb back` 命令需要权限级别 >=2 才能执行

特别地，`"root"` 指的是根命令，通常即 `!!pb` 指令

- 类型：`Dict[str, int]`

#### confirm_time_wait

有一些命令需要用户输入 `!!pb confirm` 才可继续执行。
这里定义了这些命令的最长等待时间

若等待超时，则命令将被取消执行

- 类型：[`Duration`](#duration)
- 默认值：`"1m"`

#### backup_on_restore

在回档至指定备份前，是否要自动创建一个备份。这一类备份被称为“回档前备份”，类型为临时备份

这是一道为那些傻瓜用户准备的安全保障

- 类型：`bool`
- 默认值：`true`

#### restore_countdown_sec

在回档时，关闭 Minecraft 服务器前，执行倒计时的持续秒数

- 类型：`int`
- 默认值：`10`

---

### 服务器配置

与 Minecraft 服务器交互相关的选项

```json
{
    "turn_off_auto_save": true,
    "commands": {
        "save_all_worlds": "save-all flush",
        "auto_save_off": "save-off",
        "auto_save_on": "save-on"
    },
    "saved_world_regex": [
        "Saved the game",
        "Saved the world"
    ],
    "save_world_max_wait": "10m"
}
```

#### turn_off_auto_save

是否应在进行备份之前关闭自动保存

由于关闭自动保存可以确保在备份期间，Minecraft 存档文件不会发生变化，因此建议将此选项设置为 `true`

- 类型：`bool`
- 默认值：`true`

#### commands

Prime Backup 使用的 Minecraft 指令的集合。目前它们仅在创建备份期间使用

Prime Backup 在创建备份时的操作时序如下：

1. 若配置 [`turn_off_auto_save`](#turn_off_auto_save) 为 `true` 且子配置 `auto_save_off` 为非空字符串，则使用 `auto_save_off` 存储的命令关闭自动保存
2. 若子配置 `save_all_worlds` 为非空字符串，则使用 `save_all_worlds` 存储的命令，触发服务器保存
3. 若子配置 `saved_world_regex` 非空，则开始等待至世界保存完毕。世界保存完毕指服务器的输出与 [`saved_world_regex`](#saved_world_regex) 的某一个正则表达式成功匹配
4. 创建备份
5. 如果配置 [`turn_off_auto_save`](#turn_off_auto_save) 为 `true` 且子配置 `auto_save_on` 为非空字符串，则使用 `auto_save_on` 存储的命令打开自动保存

#### saved_world_regex

一个正则表达式列表，用于识别服务器是否已保存完成

!!! note

    正则表达式将对服务端消息输出执行 [全串匹配](https://docs.python.org/3/library/re.html#re.fullmatch) 检查

- 类型：`List[re.Pattern]`

#### save_world_max_wait

在创建备份期间，等待世界保存完成的最长等待时间

- 类型：[`Duration`](#duration)
- 默认值：`"10m"`

---

### 备份配置

与创建配置具体细节相关的配置

```json
{
    "source_root": "./server",
    "source_root_use_mcdr_working_directory": false,
    "targets": [
        "world"
    ],

    "ignored_files": [],
    "ignore_patterns": [
       "**/session.lock"
    ],
    "follow_target_symlink": false,
    "reuse_stat_unchanged_file": false,
	"creation_skip_missing_file": false,
	"creation_skip_missing_file_patterns": [
       "**"
    ],

    "hash_method": "xxh128",
    "compress_method": "zstd",
    "compress_threshold": 64,

	"fileset_allocate_lookback_count": 2
}
```

#### source_root

进行备份/还原操作的根目录

通常应该是 MCDR 配置中，服务端的 [工作目录](https://mcdreforged.readthedocs.io/zh-cn/latest/configuration.html#working-directory)，
即默认情况下的 `server` 目录

- 类型：`str`
- 默认值：`"./server"`

#### source_root_use_mcdr_working_directory

若设置为 `true`，则使用 MCDR 配置中，服务端的工作路径作为 [source_root](#source_root) 的值。
此时，配置文件中 [source_root](#source_root) 键的值将被忽略

- 类型：`bool`
- 默认值：`false`

#### targets

要备份的目标文件/目录。它们也可以是 [gitignore 风格](http://git-scm.com/docs/gitignore) 的模板串

通常来讲，你需要在整理添加你的存档文件夹的名字

例如，对于像 bukkit 那样把每个维度存在独立的文件夹里的服务器，你可能需要这么配置：

```json
"targets": [
    "world",
    "world_nether",
    "world_the_end"
]
```

也可以用这个使用通配符的模板串，匹配 [source_root](#source_root) 下所有的以 "world" 开头的东西

```json
"targets": [
    "world*"
]
```

- 类型：`List[str]`

更新日志:

- v1.10.0: 支持 gitignore 风格的模板串

#### ignored_files

!!! warning

    于 v1.8.0 弃用。请使用 [ignore_patterns](#ignore_patterns)

!!! danger

    `ignored_files` 匹配的文件在回档时也不会被考虑。  
    回档后，目标文件夹中只存在已备份的内容。  
    仅对无用数据使用此选项。

在备份时忽略的文件名列表

若文件名字符串以 `*` 开头，则将忽略以指定字符串结尾的文件，
如 `*.test` 表示忽略所有以 `.test` 结尾的文件，如 `a.test`

若文件名字符串以 `*` 结尾，则将忽略以指定字符串开头的文件，
如 `temp*` 表示忽略所有以 `temp` 开头的文件，如 `tempfile`

- 类型：`List[str]`

#### ignore_patterns

!!! danger

    `ignore_patterns` 匹配的文件在回档时也不会被考虑。  
    回档后，目标文件夹中只存在已备份的内容。  
    仅对无用数据使用此选项。

一个 [gitignore 风格](http://git-scm.com/docs/gitignore) 的模板串列表，用于在创建备份的过程中匹配并忽略指定的文件 / 文件夹

模板串匹配时的根路径是 [source_root](#source_root)。
例如，如果 `source_root` 是 `server`，那么模板串 `world/trash*.obj` 将匹配 `server/world/trash1.obj`

默认包含一个 `**/session.lock` 模板串，用于匹配位于任何位置的，名为 `session.lock` 的文件，
以解决 Windows 下 `session.lock` 被服务端占用导致备份失败的问题

- 类型：`List[str]`

#### follow_target_symlink

在设为 `true` 时，对于类型为符号链接的 [备份目标](#targets)，
Prime Backup 除了会创建它们的备份外，
还会把符号链接的实际目标包括在备份目标中

例如，对于以下符号链接关系图：

```
world --> foo --> bar
^
备份目标
```

Prime Backup 除了会保存 `world` 这个符号链接外，还会保存 `foo` 符号链接，和最终的的 `bar` 文件夹

- 类型：`bool`
- 默认值：`false`

#### reuse_stat_unchanged_file

启用时，在创建备份过程中，Prime Backup 将尝试直接复用上次备份中那些状态未改变的文件。
对于这些未改变统计信息的文件，将直接复用之前备份时储存的旧文件，不再进行文件哈希检查确认

Prime Backup 会检查文件的如下这些信息。下述这些信息完全一致的文件，将被认为无变化：

- 文件路径
- 文件大小
- 文件模式位（mode）
- 文件所有者的 UID、GID
- 文件的修改时间（精度：微秒）

如果你想获得尽可能快的备份创建速度，可以尝试启用此选项，但这会引入潜在的备份不完整的风险。
除非你确实需要这一备份速度增益，或者系统磁盘读取性能过低，否则不建议启用此选项

!!! warning

    请在确保服务器的操作系统和文件系统正常且稳定运行后，再启用此选项。
    否则，如果出现系统时间回退或文件系统元数据异常等问题，可能会造成某些文件的内容变化但 stat 保持不变的情况，
    从而导致 Prime Backup 创建出了一个不完整的备份

!!! tip

    除非你确实需要这一备份速度增益，或者系统磁盘读取性能过低，否则不建议启用此选项。
    Prime Backup 的速度已经足够快了

- 类型：`bool`
- 默认值：`false`

#### creation_skip_missing_file

在某些场景下，服务端的插件 / mod 并不会遵从 /save off` 命令，
在 PB 创建备份时仍可能执行删除文件的操作。
这些缺失的文件会导致备份操作因找不到文件而失败

若你希望在这种场景下忽略这些“文件不存在”的错误，那么你可以开启该选项

另见：[creation_skip_missing_file_patterns](#creation_skip_missing_file_patterns) 选项，
用于控制允许忽略“文件不存在”错误的文件范围

- 类型：`bool`
- 默认值：`false`

#### creation_skip_missing_file_patterns

一个 [gitignore 风格](http://git-scm.com/docs/gitignore) 的模板串列表，
与 [creation_skip_missing_file_patterns](#creation_skip_missing_file_patterns) 选项配合使用

在创建备份的过程中，该选项匹配中的文件发生的“文件不存在”错误将被忽略

默认值为 `["**"]`，表示匹配所有文件。建议将其限制为仅针对那些易变的文件，如 `["trash/*.tmp"]`

- 类型：`List[str]`

#### hash_method

对文件进行哈希时所使用的算法。可用选项：`"xxh128"`、`"sha256"`、`"blake3"`

| 哈希算法                                              | 描述                                                        | 速度    | 密码学安全性       |
|---------------------------------------------------|-----------------------------------------------------------|-------|--------------|
| [`xxh128`](https://github.com/Cyan4973/xxHash)    | 一种极快的、高质量的 128 位哈希算法，不提供密码学安全性。推荐使用，除非你想要理论上的极端安全         | ★★★★★ | :cross_mark: |
| [`sha256`](https://en.wikipedia.org/wiki/SHA-2)   | 一种广泛使用的、密码学安全的 256 位哈希算法。它比 xxh128 慢，但如果 CPU 带硬件加速的话也不会太慢 | ★★    | :check_mark: |
| [`blake3`](https://github.com/BLAKE3-team/BLAKE3) | 一种高效的、密码学安全的哈希算法。比 sha256 快很多，但是依然比 xxh128 慢              | ★★★☆  | :check_mark: |

!!! note

    如果你想使用 `blake3` 作为哈希算法，你需要手动安装 `blake3` Python 库。
    它并不包含在默认的 Python 依赖列表中，因为它在某些情况下，可能需要 rust 环境来构建安装

    ```bash
    pip3 install blake3
    ```

- 类型：`str`
- 默认值：`"xxh128"`

#### compress_method

在储存备份中的文件时，所使用的压缩方法

| 压缩方法    | 描述                                                                                                    | 速度    | 压缩率   |
|---------|-------------------------------------------------------------------------------------------------------|-------|-------|
| `plain` | 无压缩，直接复制。若文件系统支持，将使用写时复制技术进行文件复制                                                                      | ★★★★★ | ☆     |
| `gzip`  | 基于 [zlib](https://www.zlib.net/) 的 [gzip](https://docs.python.org/3/library/gzip.html) 库。`.gz` 文件同款格式 | ★★    | ★★★★  |
| `lzma`  | [LZMA](https://docs.python.org/3/library/lzma.html) 算法。`.xz` 文件同款格式。提供最佳的压缩率，但是速度非常慢                  | ☆     | ★★★★★ |
| `zstd`  | [Zstandard](https://github.com/facebook/zstd) 算法。一个优秀的压缩算法，在速度和压缩率间取得了较好的平衡。推荐使用                      | ★★★☆  | ★★★★  |
| `lz4`   | [LZ4](https://github.com/lz4/lz4) 算法。比 Zstandard 快，解压速度非常快，但是压缩率相对较低                                  | ★★★★  | ★★☆   |

!!! warning

    更改 `compress_method` 只会影响新备份中的新文件（即新的数据对象）

!!! note

    如果你想使用 `lz4` 作为压缩方法，你需要手动安装 `lz4` Python 库

    ```bash
    pip3 install lz4
    ```

- 类型：`str`
- 默认值：`"zstd"`

#### compress_threshold

对于大小小于 `compress_threshold` 的文件，不启用压缩。它们将以 `plain` 格式存储

!!! warning

    更改 `compress_threshold` 只会影响新备份中的新文件（即新的数据对象）

- 类型：int
- 默认值：`64`

---

### 定时备份配置

定时备份功能的配置

该功能会定期为服务器自动创建备份

```json
{
    "enabled": false,
    "interval": "12h",
    "crontab": null,
    "jitter": "10s",
    "reset_timer_on_backup": true,
    "require_online_players": false,
    "require_online_players_blacklist": []
}
```

#### enabled, interval, crontab, jitter

见 [定时作业配置](#定时作业配置) 小节

#### reset_timer_on_backup

是否在每次手动备份时重置计划定时器

该功能仅在定时备份作业使用间隔模式的定时器的时候，才有效果

- 类型：`bool`
- 默认值：`true`

#### require_online_players

若设为 `true`，则只有在服务器中存在玩家时，才正常进行定时备份。
如果所有玩家均已离线，则在下一次定时备份完成后，之后的定时备份都会被跳过。
也就是说，Prime Backup 在没有玩家在线上时，只会触发一次定时备份

时间线举例：

```mermaid
flowchart LR
    subgraph g1[有玩家在线]
    b1[8:00\n备份创建] --> b2
    end

    subgraph g2[无玩家在线]
    b2[9:00\n备份创建] --> b3
    b3[10:00\n备份创建] --> b4:::disabled
    b4[11:00\n备份跳过] --> b5:::disabled
    end

    subgraph g3[有玩家在线]
    b5[12:00\n备份跳过] --> b6
    b6[13:00\n备份创建]
    end
    
    classDef disabled fill:#eee,stroke:#aaa,color:gray
```

!!! note

    该功能需要 Prime Backup 插件在 **服务器启动前** 就被加载。
    这是必需的，以便 Prime Backup 能够正确计算在线玩家数量

    如果 Prime Backup 插件是在服务器运行过程中被加载的，
    玩家检测功能将被禁用，就如同 [require_online_players](#require_online_players) 被设置为了 `false` 一样。

    特例：如果存在 [MinecraftDataAPI](https://github.com/MCDReforged/MinecraftDataAPI) 插件，
    那么 Prime Backup 将利用其 API 动态地查询在线玩家，此时上述需求将不再存在

!!! note

    对于那些行为表现过于不原版的服务器，Prime Backup 的玩家在线计算逻辑可能无法正常工作

!!! tip

    由于 Prime Backup 支持文件去重和高可自定义的 [清理配置](#清理配置) 功能，
    即使在玩家不在线也照常进行定时备份，也不会占用过多的磁盘空间

- 类型：`bool`
- 默认值：`false`

#### require_online_players_blacklist

在 [require_online_players](#require_online_players) 判断是否存在玩家在线时，
排除的玩家名正则表达式列表

如果你希望服务器中只有某些玩家在线的时候，也视作服务器中不存在玩家，
不进行定时备份，则可以使用该选项

配置举例：

```json
"require_online_players_blacklist": [
    "bot_.*",  // 匹配所有以 "bot_" 为前缀的玩家名
    "Steve"    // 匹配 "Steve" 这个玩家名
]
```

!!! note

    列表中的正则表达式将对玩家名执行 [全串匹配](https://docs.python.org/3/library/re.html#re.fullmatch) 检查

    匹配时 **忽略大小写**

- 类型：`List[re.Pattern]`
- 默认值：`[]`

---

### 清理配置

Prime Backup 的备份清理功能可用于自动清理过时备份

```json
{
    "enabled": true,
    "interval": "3h",
    "crontab": null,
    "jitter": "20s",
    "timezone_override": null,
    "regular_backup": {
        "enabled": false,
        "max_amount": 0,
        "max_lifetime": "0s",
        "last": -1,
        "hour": 0,
        "day": 0,
        "week": 0,
        "month": 0,
        "year": 0
    },
    "temprory_backup": {
        "enabled": true,
        "max_amount": 10,
        "max_lifetime": "30d",
        "last": -1,
        "hour": 0,
        "day": 0,
        "week": 0,
        "month": 0,
        "year": 0
    }
}
```

它包含两种清理设置，分别针对于如下两种类型的备份：

- `regular_backup`: 针对常规备份，即非临时备份
- `temporary_backup`: 针对临时备份，如回档前的备份

每种清理设置都详细描述了存档的保留策略

Prime Backup 会执行以下步骤来决定删除/保留哪些备份

1. 使用 [`last`, `hour`, `day`, `week`, `month`, `year`](#last-hour-day-week-month-year)，
   基于 [PBS](https://pbs.proxmox.com/docs/prune-simulator/) 保留策略，筛选出要删除/保留的备份
2. 使用 `max_amount`、`max_lifetime` 这两条规则，在第一步保留的那些备份里，筛出那些旧的和过期的备份
3. 收集上述两次筛选过程中，找到的哪些需要删除的备份，并逐个进行删除

删除备份相关的决策结果会以日志形式储存在 [数据根目录](#storage_root) 的 `logs/prune.log` 文件里

``` title="prune.log"
Backup #147 at 2023-12-10 21:49:09: keep=True reason=keep last 1
Backup #146 at 2023-12-10 21:36:10: keep=True reason=keep last 2
Backup #145 at 2023-12-10 21:26:25: keep=True reason=keep last 3
Backup #144 at 2023-12-10 21:21:22: keep=False reason=superseded by 145 (hour)
Backup #143 at 2023-12-10 21:16:19: keep=False reason=superseded by 145 (hour)
Backup #142 at 2023-12-10 21:11:14: keep=False reason=superseded by 145 (hour)
Backup #141 at 2023-12-10 21:05:06: keep=False reason=superseded by 145 (hour)
Backup #140 at 2023-12-10 21:00:03: keep=True reason=protected
Backup #139 at 2023-12-10 20:55:01: keep=True reason=keep hour 1
Backup #138 at 2023-12-10 20:49:57: keep=False reason=superseded by 139 (hour)
Backup #137 at 2023-12-10 20:44:53: keep=False reason=superseded by 139 (hour)
Backup #136 at 2023-12-10 20:39:45: keep=False reason=superseded by 139 (hour)
Backup #135 at 2023-12-10 20:34:41: keep=False reason=superseded by 139 (hour)
Backup #128 at 2023-12-10 19:59:06: keep=True reason=keep hour 2
Backup #116 at 2023-12-10 18:56:14: keep=True reason=keep hour 3
Backup #104 at 2023-12-10 17:55:35: keep=False reason=superseded by 116 (day)
Backup #22 at 2023-12-09 23:59:53: keep=True reason=keep day 1
```

#### max_amount

定义要保留的最大备份数量，例如 `10` 表示最多保留最新的 10 个备份

设置为 `0` 表示无限制

- 类型：`int`

#### max_lifetime

定义所有备份的最大保存时长。超出给定时长的备份将被清理

设置为 `0s` 表示无时长限制

- 类型：[`Duration`](#duration)

#### last, hour, day, week, month, year

一组 [PBS](https://pbs.proxmox.com/) 风格的清理选项，用于描述备份的删除/保留方式

查看 [清理模拟器](https://pbs.proxmox.com/docs/prune-simulator/) 了解这些选项的更多解释

[清理模拟器](https://pbs.proxmox.com/docs/prune-simulator/) 也可用于模拟备份的保留策略

注意：值 `0` 表示不为该区间保留任何备份；值 `-1` 表示该区间可以保留无限多的备份，与设为极大值等价

- 类型：`int`

---

#### enabled, interval, crontab, jitter

见 [定时作业配置](#定时作业配置) 小节

#### timezone_override

在备份清理时所使用的时区。默认情况下（使用 `null` 值），Prime Backup 将使用本地时区

例子：`null`, `"Asia/Shanghai"`, `"US/Eastern"`, `"Europe/Amsterdam"`

- 类型：`Optional[str]`
- 默认值：`null`

---

### 数据库配置

Prime Backup 所使用的 SQLite 数据库的相关配置

```json
{
    "compact": {
        "enabled": true,
        "interval": null,
        "crontab": "0 7 * * 0",
        "jitter": "1m"
    },
    "backup": {
        "enabled": true,
        "interval": null,
        "crontab": "0 6 * * 0",
        "jitter": "1m"
    }
}
```

子配置 `compact` 和 `backup` 描述了与数据库相关的定时作业

#### compact

数据库精简作业

它对数据库使用了 [VACUUM](https://www.sqlite.org/lang_vacuum.html)指令， 以精简数据库文件，并释放未使用的空间

#### backup

数据库备份作业

默认情况下，Prime Backup 会定期在 [数据根目录](#storage_root) 内的 
`db_backup` 目录中创建数据库备份，以防数据库文件损坏而导致无法访问备份

数据库备份将以 `.tar.xz` 格式存储，不会占用太多空间

#### enabled, interval, crontab, jitter

见 [定时作业配置](#定时作业配置) 小节

--- 

## 子配置项说明

### 定时作业配置

一个定时作业相关的配置，用于描述该作业会在什么时候执行。有两种模式：

- 间隔模式：按给定的时间间隔执行作业。第一次执行也要等待给定的间隔
- 定时模式: 在特定时间执行作业，由 crontab 字符串描述

若作业被启用，你必须选择上述模式之一，并正确设置相关配置值

```json
// 例子
{
    "enabled": true,
    "interval": "1h",
    "crontab": null,
    "jitter": "10s"
}
```

#### enabled

作业的开关。设为 `true` 以启用该作业，设为 `false` 以禁用该定时作业

- 类型：`bool`

#### interval

在间隔模式中使用。两次任务之间的时间间隔

若作业未使用间隔模式，其值应为 `null`

- 类型：`Optional[str]`

#### crontab

在定时模式中使用。描述定时计划的一个 crontab 字符串

你可以使用 [https://crontab.guru/](https://crontab.guru/) 来创建一个 crontab 字符串

若作业未使用定时模式，其值应为 `null`

- 类型：`Optional[str]`

#### jitter

两次作业之间，执行之间的抖动

下一个任务的实际执行时间，将在范围 `[0, jitter]` 内进行随机偏移

设置为 `"0s"` 表示无抖动

- 类型：`str`

---

## 特殊的值类型

### Duration

使用字符串描述的时间持续长度，如：`"3s"`、`"15m"`

Duration 由两部分组成：数字和时间单位。

对于数字部分，它可以是整数或浮点数

对于单位部分，参见下表：

| 单位             | 描述 | 等价于     | 秒数       |
|----------------|----|---------|----------|
| `ms`           | 毫秒 | 0.001 秒 | 0.001    |
| `s`, `sec`     | 秒  | 1 秒     | 1        |
| `m`, `min`     | 分钟 | 60 秒    | 60       |
| `h`, `hour`    | 小时 | 60 分钟   | 3600     |
| `d`, `day`     | 天  | 24 小时   | 86400    |
| `mon`, `month` | 月  | 30 天    | 2592000  |
| `y`, `year`    | 年  | 365 天   | 31536000 |
