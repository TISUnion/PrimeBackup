from typing import Optional, List

from sqlalchemy import String, BIGINT, Integer, Date, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
	pass


class Meta(Base):
	__tablename__ = 'meta'

	version: Mapped[int] = mapped_column(primary_key=True)


class Blob(Base):
	__tablename__ = 'blob'

	hash: Mapped[str] = mapped_column(String(130), primary_key=True)
	compress: Mapped[str] = mapped_column(String)
	size: Mapped[int] = mapped_column(BIGINT)
	ref_cnt: Mapped[int] = mapped_column(Integer)

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

	blob: Mapped[List['Blob']] = relationship(back_populates='files')
	backup: Mapped[List['Backup']] = relationship(back_populates='files')


class Backup(Base):
	__tablename__ = 'backup'

	id = mapped_column(Integer, primary_key=True, autoincrement=True)
	date = mapped_column(Date)
	comment = mapped_column(String)

	files: Mapped[List['File']] = relationship(back_populates='backup')
