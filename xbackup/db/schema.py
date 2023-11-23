from typing import Optional, List, get_type_hints

from sqlalchemy import String, BIGINT, Integer, ForeignKey, BigInteger
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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


class DbVersion(Base):
	__tablename__ = 'db_version'

	version: Mapped[int] = mapped_column(primary_key=True)


class Blob(Base):
	__tablename__ = 'blob'

	hash: Mapped[str] = mapped_column(String(130), primary_key=True)
	compress: Mapped[str] = mapped_column(String)
	size: Mapped[int] = mapped_column(BIGINT, index=True)

	__fields_end__: bool

	files: Mapped[List['File']] = relationship(back_populates='blob')


class File(Base):
	__tablename__ = 'file'

	backup_id: Mapped[str] = mapped_column(ForeignKey("backup.id"), primary_key=True)
	path: Mapped[str] = mapped_column(String, primary_key=True)

	file_hash: Mapped[Optional[str]] = mapped_column(ForeignKey("blob.hash"))
	mode: Mapped[int] = mapped_column(Integer)

	uid: Mapped[Optional[int]] = mapped_column(Integer)
	gid: Mapped[Optional[int]] = mapped_column(Integer)
	mtime_ns: Mapped[Optional[int]] = mapped_column(BIGINT)
	ctime_ns: Mapped[Optional[int]] = mapped_column(BIGINT)

	__fields_end__: bool

	blob: Mapped['Blob'] = relationship(back_populates='files')
	backup: Mapped[List['Backup']] = relationship(back_populates='files')


class Backup(Base):
	__tablename__ = 'backup'

	id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
	timestamp: Mapped[int] = mapped_column(BigInteger)  # timestamp in millisecond
	author: Mapped[str] = mapped_column(String)
	comment: Mapped[str] = mapped_column(String)

	__fields_end__: bool

	files: Mapped[List['File']] = relationship(back_populates='backup')
