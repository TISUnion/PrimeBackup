---
title: '任务通知与 Webhook'
---

Prime Backup 支持在备份/回档任务的关键节点主动推送通知，便于无人值守或自动化运维场景下及时获知任务状态

## 支持的事件类型

- `backup_start`：备份开始
- `backup_success`：备份成功
- `backup_failure`：备份失败
- `restore_start`：回档开始
- `restore_success`：回档成功
- `restore_failure`：回档失败

## 配置示例

```json
{
    "notification": {
        "enabled": true,
        "events": [
            "backup_start",
            "backup_success",
            "backup_failure",
            "restore_start",
            "restore_success",
            "restore_failure"
        ],
        "endpoints": [
            {
                "enabled": true,
                "name": "webhook",
                "type": "webhook",
                "url": "https://example.com/webhook",
                "headers": {
                    "Authorization": "Bearer token"
                },
                "timeout": "5s"
            }
        ]
    }
}
```

## Payload 结构

Webhook 使用 `POST` JSON 请求，常见字段如下：

- `event`：事件类型
- `task`：任务类型（`backup` / `restore`）
- `status`：状态（`start` / `success` / `failure`）
- `timestamp`：时间戳（`unix` 与 `iso`）
- `backup`：备份信息（id、comment、date、tags、大小等）
- `operator` / `source`：操作者与触发来源
- `message` / `error`：失败原因或异常信息（如有）
- `title` / `body` / `desp`：便于第三方服务直接展示的简要内容

## 推送通道与负载说明

- `type = webhook`：发送完整 Prime Backup JSON payload（用于通用 Webhook）
- `type = bark`：发送 Bark 原生 JSON payload（字段名与 Bark 接口一致）

### Bark 负载默认规则

- 默认会根据任务内容生成 `title` / `body`
- `body` 会包含运维关键信息（事件/状态/备份ID/注释/操作者/耗时/错误等）
- 默认等级：成功 `passive`，失败 `critical`，开始/其他 `active`

## 与常见服务集成

### Bark

Prime Backup 已提供 Bark 原生支持，无需中转 Webhook。配置时将 Endpoint 的 `type` 设为 `bark` 即可

推荐使用 **Key 放在 URL 路径** 的方式（无需 `device_key` 字段）：

```json
{
    "notification": {
        "enabled": true,
        "events": ["backup_success", "backup_failure"],
        "endpoints": [
            {
                "name": "bark",
                "type": "bark",
                "url": "https://api.day.app/<your_key>",
                "timeout": "5s"
            }
        ]
    }
}
```

适合运维的高级配置示例（Bark 原生字段）：

```json
{
    "notification": {
        "enabled": true,
        "events": ["backup_start", "backup_success", "backup_failure", "restore_success", "restore_failure"],
        "endpoints": [
            {
                "name": "bark",
                "type": "bark",
                "url": "https://api.day.app/<your_key>",
                "timeout": "5s",
                "bark": {
                    "group": "prime_backup",
                    "sound": "minuet",
                    "badge": 1,
                    "url": "https://example.com/ops/runbook",
                    "icon": "https://example.com/icon.png"
                }
            }
        ]
    }
}
```

若使用 `/push` 形式，可在 `bark.device_key` 中提供 key：

```json
{
    "notification": {
        "enabled": true,
        "events": ["backup_success", "backup_failure"],
        "endpoints": [
            {
                "name": "bark",
                "type": "bark",
                "url": "https://api.day.app/push",
                "timeout": "5s",
                "bark": {
                    "device_key": "<your_key>",
                    "group": "prime_backup",
                    "level": "timeSensitive",
                    "sound": "minuet",
                    "badge": 1
                }
            }
        ]
    }
}
```

`bark` 中支持 Bark 的大部分参数（如 `group` / `sound` / `level` / `badge` / `icon` / `url` / `id` / `delete` 等），
Prime Backup 会将 `title` / `body` 自动填入（也可通过 `bark.title` / `bark.body` 覆盖）

为便于运维查看，Bark 的 `body` 默认会包含关键字段（事件、状态、备份ID、注释、操作者、耗时、错误等），
且会根据结果自动设置通知等级：

- 成功：`passive`
- 失败：`critical`
- 开始/其他：`active`

如需自定义，可显式设置 `bark.level` 或覆盖 `bark.body` / `bark.markdown`

## 测试通知

可使用命令发送测试通知：

- `!!pb test notify`
- `!!pb test notify <event>`

### Server酱

Server酱的 API 通常使用 `title` 与 `desp` 字段，Prime Backup 的 Payload 会同时提供这些字段：

```json
{
    "notification": {
        "enabled": true,
        "events": ["backup_success", "backup_failure"],
        "endpoints": [
            {
                "name": "serverchan",
                "url": "https://sctapi.ftqq.com/<your_key>.send",
                "timeout": "5s"
            }
        ]
    }
}
```

如需更复杂的格式或自定义字段，可使用中转 Webhook 服务进行二次处理
