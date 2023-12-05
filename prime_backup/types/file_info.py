import stat
from typing import NamedTuple, Optional

from prime_backup.db import schema
from prime_backup.types.blob_info import BlobInfo


class FileInfo(NamedTuple):
	backup_id: int
	path: str

	mode: int
	content: Optional[bytes]
	blob: BlobInfo

	uid: Optional[int]
	gid: Optional[int]
	ctime_ns: Optional[int]
	mtime_ns: Optional[int]
	atime_ns: Optional[int]

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

	def is_file(self) -> bool:
		return stat.S_ISREG(self.mode)

	def is_dir(self) -> bool:
		return stat.S_ISDIR(self.mode)

	def is_link(self) -> bool:
		return stat.S_ISLNK(self.mode)
