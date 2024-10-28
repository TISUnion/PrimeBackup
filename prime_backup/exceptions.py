from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
	from prime_backup.types.blob_info import BlobInfo


class PrimeBackupError(Exception):
	pass


class BackupNotFound(PrimeBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class BackupFileNotFound(PrimeBackupError):
	def __init__(self, file_set_id: int, path: str):
		super().__init__()
		self.file_set_id = file_set_id
		self.path = path


class FileSetNotFound(PrimeBackupError):
	def __init__(self, file_set_id: int):
		super().__init__()
		self.file_set_id = file_set_id


class BlobNotFound(PrimeBackupError):
	def __init__(self, blob_hash: str):
		super().__init__()
		self.blob_hash = blob_hash


class BlobHashNotUnique(PrimeBackupError):
	def __init__(self, blob_hash_prefix: str, candidates: List['BlobInfo']):
		super().__init__()
		self.blob_hash_prefix = blob_hash_prefix
		self.candidates = candidates


class UnsupportedFileFormat(PrimeBackupError):
	def __init__(self, mode: int):
		self.mode = mode


class VerificationError(PrimeBackupError):
	pass
