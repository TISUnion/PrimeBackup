import enum
from typing import Optional, List, get_type_hints, Dict, Any

from sqlalchemy import String, Integer, ForeignKey, BigInteger, JSON, LargeBinary, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

BackupTagDict = Dict[str, Any]


class Base(DeclarativeBase):
	def __repr__(self) -> str:
		cls = self.__class__
		values = {}
		for name, type_ in get_type_hints(cls).items():
			if name == '__fields_end__':
				break
			if not name.startswith('_') and getattr(type_, '__origin__') == Mapped:
				values[name] = getattr(self, name)
		return '{}({})'.format(cls.__name__, ', '.join([f'{k}={v!r}' for k, v in values.items()]))


class DbMeta(Base):
	__tablename__ = 'db_meta'

	magic: Mapped[int] = mapped_column(Integer, primary_key=True)
	version: Mapped[int] = mapped_column(Integer)
	hash_method: Mapped[str] = mapped_column(String)


class Blob(Base):
	__tablename__ = 'blob'

	hash: Mapped[str] = mapped_column(String, primary_key=True)
	compress: Mapped[str] = mapped_column(String)
	raw_size: Mapped[int] = mapped_column(BigInteger, index=True)
	stored_size: Mapped[int] = mapped_column(BigInteger)

	__fields_end__: bool


class FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4


class File(Base):
	__tablename__ = 'file'

	fileset_id: Mapped[int] = mapped_column(ForeignKey('backup.id'), primary_key=True, index=True)
	path: Mapped[str] = mapped_column(String, primary_key=True)
	role: Mapped[str] = mapped_column(Integer)  # see enum FileRole

	mode: Mapped[int] = mapped_column(Integer)

	# whole file content for special files, e.g. target of symlink
	content: Mapped[Optional[bytes]] = mapped_column(LargeBinary)

	# store all Blob fields here to speed up blob attribute accessing
	blob_hash: Mapped[Optional[str]] = mapped_column(ForeignKey('blob.hash'), index=True)
	blob_compress: Mapped[Optional[str]] = mapped_column(String)
	blob_raw_size: Mapped[Optional[int]] = mapped_column(BigInteger)
	blob_stored_size: Mapped[Optional[int]] = mapped_column(BigInteger)

	uid: Mapped[Optional[int]] = mapped_column(Integer)
	gid: Mapped[Optional[int]] = mapped_column(Integer)
	ctime_ns: Mapped[Optional[int]] = mapped_column(BigInteger)
	mtime_ns: Mapped[Optional[int]] = mapped_column(BigInteger)
	atime_ns: Mapped[Optional[int]] = mapped_column(BigInteger)

	__fields_end__: bool


class Fileset(Base):
	__tablename__ = 'fileset'
	__table_args__ = {'sqlite_autoincrement': True}

	id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, index=True)
	base: Mapped[bool] = mapped_column(Boolean)

	# Store common statistics data of backup files
	file_raw_size_sum: Mapped[Optional[int]] = mapped_column(BigInteger)
	file_stored_size_sum: Mapped[Optional[int]] = mapped_column(BigInteger)

	__fields_end__: bool


class Backup(Base):
	__tablename__ = 'backup'
	__table_args__ = {'sqlite_autoincrement': True}

	id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, index=True)
	timestamp: Mapped[int] = mapped_column(BigInteger)  # timestamp in nanosecond
	creator: Mapped[str] = mapped_column(String)
	comment: Mapped[str] = mapped_column(String)
	targets: Mapped[List[str]] = mapped_column(JSON)
	tags: Mapped[BackupTagDict] = mapped_column(JSON)

	# fileset ids
	fileset_id_base: Mapped[int] = mapped_column(ForeignKey('fileset.id'))
	fileset_id_delta: Mapped[int] = mapped_column(ForeignKey('fileset.id'))

	# Store common statistics data of backup files
	file_raw_size_sum: Mapped[Optional[int]] = mapped_column(BigInteger)
	file_stored_size_sum: Mapped[Optional[int]] = mapped_column(BigInteger)

	__fields_end__: bool

	fileset_base: Mapped['Fileset'] = relationship(viewonly=True, foreign_keys=[fileset_id_base])
	fileset_delta: Mapped['Fileset'] = relationship(viewonly=True, foreign_keys=[fileset_id_delta])
