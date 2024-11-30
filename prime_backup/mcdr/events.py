from mcdreforged.api.event import LiteralEvent

from prime_backup.constants import PLUGIN_ID

# events
BACKUP_DONE_EVENT 		= LiteralEvent('{}.backup_done'.format(PLUGIN_ID))  # -> source, backup_id
RESTORE_DONE_EVENT 		= LiteralEvent('{}.restore_done'.format(PLUGIN_ID))  # -> source, backup_id
DELETE_DONE_EVENT 		= LiteralEvent('{}.delete_done'.format(PLUGIN_ID))  # -> source, backup_id
EXPORT_DONE_EVENT 		= LiteralEvent('{}.export_done'.format(PLUGIN_ID))  # -> source, backup_id, file_path
IMPORT_DONE_EVENT 		= LiteralEvent('{}.import_done'.format(PLUGIN_ID))  # -> source, backup_id
TRIGGER_BACKUP_EVENT 	= LiteralEvent('{}.trigger_backup'.format(PLUGIN_ID))  # <- source, comment, operator
TRIGGER_RESTORE_EVENT 	= LiteralEvent('{}.trigger_restore'.format(PLUGIN_ID))  # <- source, backup_id, needs_confirm, fail_soft, verify_blob
TRIGGER_DELETE_EVENT 	= LiteralEvent('{}.trigger_delete'.format(PLUGIN_ID))  # <- source, [backup_id]
TRIGGER_EXPORT_EVENT 	= LiteralEvent('{}.trigger_export'.format(PLUGIN_ID))  # <- source, backup_id, export_format, fail_soft, verify_blob, overwrite_existing, create_meta
TRIGGER_IMPORT_EVENT 	= LiteralEvent('{}.trigger_import'.format(PLUGIN_ID))  # <- source, file_path, backup_format, ensure_meta, meta_override