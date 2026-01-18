---
title: 'Task Notifications & Webhook'
---

Prime Backup can push notifications for backup/restore lifecycle events, which is useful for unattended ops

## Supported events

- `backup_start`
- `backup_success`
- `backup_failure`
- `restore_start`
- `restore_success`
- `restore_failure`

## Configuration example

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

## Payload

For `type = webhook`, Prime Backup sends a JSON payload that includes:

- `event`, `task`, `status`
- `timestamp`
- `backup` (id/comment/date/tags/size)
- `operator` / `source`
- `message` / `error`
- `title` / `body` / `desp`

## Bark

Prime Backup has native Bark support. Set endpoint `type` to `bark`

Key in URL (recommended):

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

Using `/push` + `device_key`:

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

Bark body defaults to ops-friendly details, and levels are mapped automatically:

- success: `passive`
- failure: `critical`
- start/others: `active`

You can override with `bark.level` or `bark.body` / `bark.markdown`

## Test notification

- `!!pb test notify`
- `!!pb test notify <event>`
