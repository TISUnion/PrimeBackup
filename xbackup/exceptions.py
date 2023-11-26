class XBackupError(Exception):
	pass


class BackupNotFound(XBackupError):
	pass


class Timeout(XBackupError):
	pass
