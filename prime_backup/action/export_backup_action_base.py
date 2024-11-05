import json
from abc import abstractmethod, ABC
from pathlib import Path

from prime_backup.action import Action
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.exceptions import PrimeBackupError, VerificationError
from prime_backup.types.backup_meta import BackupMeta
from prime_backup.types.export_failure import ExportFailures
from prime_backup.utils import misc_utils


class _ExportBackupActionBase(Action[ExportFailures], ABC):
	LOG_FILE_CREATION = False

	class _ExportInterrupted(PrimeBackupError):
		pass

	def __init__(
			self, backup_id: int, output_path: Path, *,
			fail_soft: bool = False, verify_blob: bool = True, create_meta: bool = True,
	):
		super().__init__()
		self.backup_id = misc_utils.ensure_type(backup_id, int)
		self.output_path = output_path
		self.fail_soft = fail_soft
		self.verify_blob = verify_blob
		self.create_meta = create_meta

	def run(self) -> ExportFailures:
		with DbAccess.open_session() as session:
			backup = session.get_backup(self.backup_id)
			failures = self._export_backup(session, backup)

		if len(failures) > 0:
			self.logger.info('Export done with {} failures'.format(len(failures)))
		else:
			self.logger.info('Export done')
		return failures

	@abstractmethod
	def _export_backup(self, session: DbSession, backup: schema.Backup) -> ExportFailures:
		...

	def _create_meta_buf(self, backup: schema.Backup) -> bytes:
		if not self.create_meta:
			raise RuntimeError('calling _create_meta_buf() with create_meta set to False')
		meta = BackupMeta.from_backup(backup)
		return json.dumps(meta.to_dict(), indent=2, ensure_ascii=False).encode('utf8')

	@classmethod
	def _on_unsupported_file_mode(cls, file: schema.File):
		raise NotImplementedError('file at {!r} with mode={} ({} or {}) is not supported yet'.format(file.path, file.mode, hex(file.mode), oct(file.mode)))

	@classmethod
	def _verify_exported_blob(cls, file: schema.File, written_size: int, written_hash: str):
		if written_size != file.blob_raw_size:
			raise VerificationError('raw size mismatched for {}, expected {}, actual written {}'.format(file.path, file.blob_raw_size, written_size))
		if written_hash != file.blob_hash:
			raise VerificationError('hash mismatched for {}, expected {}, actual written {}'.format(file.path, file.blob_hash, written_hash))
