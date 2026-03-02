import contextlib
import stat
import tarfile
import time
from io import BytesIO
from pathlib import Path
from typing import Union, BinaryIO, Generator

from typing_extensions import override, Unpack

from prime_backup.action.export_backup_action_base import _ExportBackupActionBase, ExportBackupActionCommonInitKwargs
from prime_backup.action.helpers.blob_exporter import BlobChunksGetter, ThreadSafeBlobChunksGetter
from prime_backup.compressors import Compressor
from prime_backup.constants.constants import BACKUP_META_FILE_NAME
from prime_backup.db import schema
from prime_backup.types.export_failure import ExportFailures
from prime_backup.types.tar_format import TarFormat
from prime_backup.utils import platform_utils
from prime_backup.utils.io_types import SupportsReadBytes


class ExportBackupToTarAction(_ExportBackupActionBase):
	def __init__(self, backup_id: int, output_dest: Union[Path, BinaryIO], tar_format: TarFormat, **kwargs: Unpack[ExportBackupActionCommonInitKwargs]):
		super().__init__(backup_id, **kwargs)
		self.output_dest = output_dest
		self.tar_format = tar_format

	@override
	def is_interruptable(self) -> bool:
		return True

	@contextlib.contextmanager
	def __open_tar(self) -> Generator[tarfile.TarFile, None, None]:
		with contextlib.ExitStack() as es:
			f: BinaryIO
			if isinstance(self.output_dest, Path):
				f = es.enter_context(open(self.output_dest, 'wb'))
			else:
				f = self.output_dest

			compressor = Compressor.create(self.tar_format.value.compress_method)
			with compressor.compress_stream(f) as f_compressed:
				with tarfile.open(fileobj=f_compressed, mode=self.tar_format.value.mode_w) as tar:
					yield tar

	def __export_file(self, blob_chunks_getter: BlobChunksGetter, tar: tarfile.TarFile, file: schema.File):
		info = tarfile.TarInfo(name=file.path)
		info.mode = file.mode

		if file.uid is not None:
			info.uid = file.uid
			if (uid_name := platform_utils.uid_to_name(int(file.uid))) is not None:
				info.uname = uid_name
		if file.gid is not None:
			info.gid = file.gid
			if (gid_name := platform_utils.gid_to_name(int(file.gid))) is not None:
				info.gname = gid_name
		if file.mtime is not None:
			info.mtime = file.mtime_unix_sec
		if stat.S_ISREG(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add file {} to tarfile'.format(file.path))
			if file.blob_raw_size is None:
				raise AssertionError('file.blob_raw_size is None for file {!r}'.format(file))
			info.type = tarfile.REGTYPE
			info.size = file.blob_raw_size

			def reader_csm(reader: SupportsReadBytes):
				tar.addfile(tarinfo=info, fileobj=reader)

			self._create_blob_exporter(blob_chunks_getter, file).export_as_reader(reader_csm)
		elif stat.S_ISDIR(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add dir {} to tarfile'.format(file.path))
			info.type = tarfile.DIRTYPE
			tar.addfile(tarinfo=info)
		elif stat.S_ISLNK(file.mode):
			if self.LOG_FILE_CREATION:
				self.logger.debug('add symlink {} to tarfile'.format(file.path))
			if not file.content:
				raise AssertionError('symlink file {} has no content'.format(file))
			link_target = file.content.decode('utf8')
			info.type = tarfile.SYMTYPE
			info.linkname = link_target
			tar.addfile(tarinfo=info)
		else:
			self._on_unsupported_file_mode(file)

	@override
	def _export_backup(self, session, backup: schema.Backup) -> ExportFailures:
		failures = ExportFailures(self.fail_soft)

		if isinstance(self.output_dest, Path):
			if not self.output_dest.name.endswith(self.tar_format.value.extension):
				raise ValueError('bad output file extension for file name {!r}, should be {!r} for tar format {}'.format(
					self.output_dest.name, self.tar_format.value.extension, self.tar_format.name,
				))

			self.logger.info('Exporting backup {} to tarfile {}'.format(backup, self.output_dest))
			self.output_dest.parent.mkdir(parents=True, exist_ok=True)
		else:
			self.logger.info('Exporting backup {} to given BinaryIO object'.format(backup))

		ts_bcg = ThreadSafeBlobChunksGetter(session)
		try:
			with self.__open_tar() as tar:
				for file in session.get_backup_files(backup):
					if self.is_interrupted.is_set():
						self.logger.info('Export to tarfile interrupted')
						raise self._ExportInterrupted()

					with failures.handling_exception(file):
						try:
							self.__export_file(ts_bcg, tar, file)
						except Exception as e:
							output_dest_str = str(self.output_dest) if isinstance(self.output_dest, Path) else str(type(self.output_dest))
							self.logger.error('Export file {!r} to tar {} failed: {}'.format(file.path, output_dest_str, e))
							raise

				if self.create_meta:
					meta_buf = self._create_meta_buf(backup)
					info = tarfile.TarInfo(name=BACKUP_META_FILE_NAME)
					info.mtime = int(time.time())
					info.size = len(meta_buf)
					tar.addfile(tarinfo=info, fileobj=BytesIO(meta_buf))
		except Exception as e:
			if isinstance(self.output_dest, Path):
				with contextlib.suppress(OSError):
					self.output_dest.unlink(missing_ok=True)
			if not isinstance(e, self._ExportInterrupted):
				raise

		return failures
