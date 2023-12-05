import dataclasses
import enum
import functools
import stat
from typing import Optional

from prime_backup.db import schema
from prime_backup.types.blob_info import BlobInfo


class FileType(enum.Enum):
	file = enum.auto()
	directory = enum.auto()
	symlink = enum.auto()
	unknown = enum.auto()


@dataclasses.dataclass(frozen=True)
class FileInfo:
	backup_id: int
	path: str

	mode: int
	content: Optional[bytes] = None
	blob: Optional[BlobInfo] = None

	uid: Optional[int] = None
	gid: Optional[int] = None
	ctime_ns: Optional[int] = None
	mtime_ns: Optional[int] = None
	atime_ns: Optional[int] = None

	@classmethod
	def of(cls, file: schema.File) -> 'FileInfo':
		"""
		Notes: should be inside a session
		"""
		return FileInfo(
			backup_id=file.backup_id,
			path=file.path,
			mode=file.mode,
			content=file.content,
			blob=BlobInfo(
				hash=file.blob_hash,
				compress=file.blob_compress,
				raw_size=file.blob_raw_size,
				stored_size=file.blob_stored_size,
			),
			uid=file.uid,
			gid=file.gid,
			ctime_ns=file.ctime_ns,
			mtime_ns=file.mtime_ns,
			atime_ns=file.atime_ns,
		)

	@functools.cached_property
	def file_type(self) -> FileType:
		if self.is_file():
			return FileType.file
		elif self.is_dir():
			return FileType.directory
		elif self.is_link():
			return FileType.symlink
		else:
			return FileType.unknown

	def is_file(self) -> bool:
		return stat.S_ISREG(self.mode)

	def is_dir(self) -> bool:
		return stat.S_ISDIR(self.mode)

	def is_link(self) -> bool:
		return stat.S_ISLNK(self.mode)
