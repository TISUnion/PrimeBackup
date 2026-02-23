from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
	from prime_backup.types.blob_info import BlobInfo


class PrimeBackupError(Exception):
	pass


class BackupNotFound(PrimeBackupError):
	def __init__(self, backup_id: int):
		super().__init__()
		self.backup_id = backup_id


class OffsetBackupNotFound(PrimeBackupError):
	def __init__(self, offset: int):
		self.offset = offset


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


class FilesetNotFound(PrimeBackupError):
	def __init__(self, fileset_id: int):
		super().__init__()
		self.fileset_id = fileset_id


class BlobNotFound(PrimeBackupError):
	pass


class BlobIdNotFound(BlobNotFound):
	def __init__(self, blob_id: int):
		super().__init__()
		self.blob_id = blob_id


class BlobHashNotFound(BlobNotFound):
	def __init__(self, blob_hash: str):
		super().__init__()
		self.blob_hash = blob_hash


class BlobHashNotUnique(PrimeBackupError):
	def __init__(self, blob_hash_prefix: str, candidates: List['BlobInfo']):
		super().__init__()
		self.blob_hash_prefix = blob_hash_prefix
		self.candidates = candidates


class ChunkNotFound(PrimeBackupError):
	pass


class ChunkIdNotFound(ChunkNotFound):
	def __init__(self, chunk_id: int):
		super().__init__()
		self.chunk_id = chunk_id


class ChunkHashNotFound(ChunkNotFound):
	def __init__(self, chunk_hash: str):
		super().__init__()
		self.chunk_hash = chunk_hash


class ChunkGroupNotFound(PrimeBackupError):
	pass


class ChunkGroupIdNotFound(ChunkGroupNotFound):
	def __init__(self, chunk_group_id: int):
		super().__init__()
		self.chunk_group_id = chunk_group_id


class ChunkGroupHashNotFound(ChunkGroupNotFound):
	def __init__(self, chunk_group_hash: str):
		super().__init__()
		self.chunk_group_hash = chunk_group_hash


class ChunkGroupChunkBindingNotFound(PrimeBackupError):
	def __init__(self, chunk_group_id: int, chunk_offset: int):
		super().__init__()
		self.chunk_group_id = chunk_group_id
		self.chunk_offset = chunk_offset


class BlobChunkGroupBindingNotFound(PrimeBackupError):
	def __init__(self, blob_id: int, chunk_group_offset: int):
		super().__init__()
		self.blob_id = blob_id
		self.chunk_group_offset = chunk_group_offset


class UnsupportedFileFormat(PrimeBackupError):
	def __init__(self, mode: int):
		self.mode = mode


class VerificationError(PrimeBackupError):
	pass
