from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
	from prime_backup.types.blob_info import BlobInfo


class PrimeBackupError(Exception):
	pass


class BackupNotFound(PrimeBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class BaseFileNotFound(PrimeBackupError):
	def __init__(self, path: str):
		self.path = path


class BackupFileNotFound(BaseFileNotFound):
	def __init__(self, backup_id: int, path: str):
		super().__init__(path)
		self.backup_id = backup_id


class FilesetFileNotFound(BaseFileNotFound):
	def __init__(self, fileset_id: int, path: str):
		super().__init__(path)
		self.fileset_id = fileset_id


class FileSetNotFound(PrimeBackupError):
	def __init__(self, fileset_id: int):
		super().__init__()
		self.fileset_id = fileset_id


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
