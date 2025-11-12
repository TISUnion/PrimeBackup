import dataclasses
import enum
import functools
import stat
from typing import Optional, Tuple, List, TYPE_CHECKING

from typing_extensions import Self

from prime_backup.compressors import CompressMethod
from prime_backup.db import schema
from prime_backup.db.values import FileRole
from prime_backup.types.blob_info import BlobInfo, BlobListSummary
from prime_backup.utils import misc_utils

if TYPE_CHECKING:
	from prime_backup.types.backup_info import BackupInfo


class FileType(enum.Enum):
	file = enum.auto()
	directory = enum.auto()
	symlink = enum.auto()
	unknown = enum.auto()


# TODO: FileIdentifier?
FileUniqueKey = Tuple[int, str]  # (fileset_id, path)


@dataclasses.dataclass(frozen=True)
class FileInfo:
	fileset_id: int
	path: str
	role: FileRole

	mode: int
	content: Optional[bytes] = None
	blob: Optional[BlobInfo] = None

	uid: Optional[int] = None
	gid: Optional[int] = None
	mtime_us: Optional[int] = None

	# optional stats
	# Backup samples below do contain this file, i.e. this file is not override by another delta fileset file
	backup_count: int = 0
	backup_samples: List['BackupInfo'] = dataclasses.field(default_factory=list)

	@classmethod
	def of(cls, file: schema.File, *, backup_count: int = 0, backup_samples: Optional[List[schema.Backup]] = None) -> 'FileInfo':
		"""
		Notes: should be inside a session
		"""
		blob: Optional[BlobInfo] = None
		if file.blob_hash is not None:
			if file.blob_compress not in CompressMethod.__members__:
				from prime_backup import logger
				logger.get().warning('Bad blob_compress {!r} for file {!r}'.format(file.blob_compress, file))
			else:
				blob = BlobInfo(
					hash=str(file.blob_hash),
					compress=CompressMethod[file.blob_compress],
					raw_size=file.blob_raw_size,
					stored_size=file.blob_stored_size,
				)
		try:
			role = FileRole(file.role)
		except (KeyError, ValueError):
			role = FileRole.unknown

		from prime_backup.types.backup_info import BackupInfo
		return FileInfo(
			fileset_id=file.fileset_id,
			path=file.path,
			role=role,
			mode=file.mode,
			content=file.content,
			blob=blob,
			uid=file.uid,
			gid=file.gid,
			mtime_us=file.mtime,
			backup_count=backup_count,
			backup_samples=[BackupInfo.of(backup) for backup in backup_samples] if backup_samples is not None else [],
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

	@functools.cached_property
	def content_str(self) -> Optional[str]:
		"""
		Decode the content with utf8
		"""
		if self.content is None:
			return None
		if not self.is_link():
			raise AssertionError('should only access the str form of the file content for symlink files, not for {}'.format(self.file_type))
		return self.content.decode('utf8')

	@functools.cached_property
	def __cmp_key(self) -> tuple:
		parts = [(part.lower(), part) for part in self.path.split('/')]
		return self.fileset_id, *parts

	def __lt__(self, other: 'FileInfo') -> bool:
		return self.__cmp_key < other.__cmp_key

	@property
	def unique_key(self) -> FileUniqueKey:
		return self.fileset_id, self.path


@dataclasses.dataclass
class FileListSummary:
	count: int
	blob_summary: BlobListSummary

	@classmethod
	def zero(cls) -> Self:
		return cls(0, BlobListSummary.zero())

	def __add__(self, other: Self) -> Self:
		misc_utils.ensure_type(other, type(self))
		return FileListSummary(
			count=self.count + other.count,
			blob_summary=self.blob_summary + other.blob_summary,
		)
