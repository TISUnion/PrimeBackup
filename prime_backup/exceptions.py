class PrimeBackupError(Exception):
	pass


class BackupNotFound(PrimeBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class BackupFileNotFound(PrimeBackupError):
	def __init__(self, backup_id: int, path: str):
		super().__init__()
		self.backup_id = backup_id
		self.path = path


class Timeout(PrimeBackupError):
	pass
