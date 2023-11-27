class PrimeBackupError(Exception):
	pass


class BackupNotFound(PrimeBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class Timeout(PrimeBackupError):
	pass
