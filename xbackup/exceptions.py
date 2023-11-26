class XBackupError(Exception):
	pass


class BackupNotFound(XBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class Timeout(XBackupError):
	pass
