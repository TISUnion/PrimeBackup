# events
from mcdreforged.plugin.plugin_event import LiteralEvent

from prime_backup.constants import PLUGIN_ID

BACKUP_DONE_EVENT 		= LiteralEvent('{}.backup_done'.format(PLUGIN_ID))  # -> source, id: int
TRIGGER_BACKUP_EVENT 	= LiteralEvent('{}.trigger_backup'.format(PLUGIN_ID))  # <- source, comment
