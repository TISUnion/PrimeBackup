import uuid

PLUGIN_ID = 'prime_backup'
INSTANCE_ID = uuid.uuid4().hex[:4]

DEFAULT_COMMAND_PERMISSION_LEVEL = 1

BACKUP_META_FILE_NAME = '.prime_backup.meta.json'
