import contextlib
from typing import List, Dict, Optional, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.action.delete_blob_action import DeleteOrphanBlobsAction
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import FileIdentifier, DbSession
from prime_backup.exceptions import FilesetFileNotFound
from prime_backup.types.file_info import FileListSummary


class DeleteFilesStep(Action[FileListSummary]):
	def __init__(self, session: DbSession, files: List[schema.File]):
		super().__init__()
		self.session = session
		self.files = files
		self.__has_run = False

	def run(self) -> FileListSummary:
		"""
		`session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		if self.__has_run:
			raise RuntimeError('no double run')
		self.__has_run = True

		deleted_blob_hashes: Set[str] = set()
		for file in self.files:
			if file.blob_hash is not None:
				deleted_blob_hashes.add(file.blob_hash)
			self.session.delete_file(file)
		deleted_file_count = len(self.files)

		action = DeleteOrphanBlobsAction(deleted_blob_hashes)
		bls = action.run(session=self.session)

		return FileListSummary(count=deleted_file_count, blob_summary=bls)


class DeleteFilesAction(Action[FileListSummary]):
	def __init__(self, file_identifiers: List[FileIdentifier], *, raise_if_not_found: bool = True):
		super().__init__()
		self.file_identifiers = file_identifiers
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self, *, session: Optional[DbSession] = None) -> FileListSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			files: Dict[FileIdentifier, Optional[schema.File]] = session.get_file_objects_opt(self.file_identifiers)
			self_file_identifiers_set = set(self.file_identifiers)
			collected_files: List[schema.File] = []

			for file_identifier, file in files.items():
				if file is None and self.raise_if_not_found:
					raise FilesetFileNotFound(file_identifier.fileset_id, file_identifier.path)
				else:
					if file_identifier not in self_file_identifiers_set:
						raise AssertionError('got unexpected file_identifier {!r}, should be in {}'.format(file_identifier, self_file_identifiers_set))
					collected_files.append(file)

			return DeleteFilesStep(session, collected_files).run()
