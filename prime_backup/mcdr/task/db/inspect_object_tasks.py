import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Any

from mcdreforged.api.all import *

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.action.get_blob_action import GetBlobByHashPrefixAction
from prime_backup.action.get_file_action import GetBackupFileAction, GetFilesetFileAction
from prime_backup.action.get_fileset_action import GetFilesetAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.file_info import FileInfo
from prime_backup.utils import platform_utils
from prime_backup.utils.mcdr_utils import TranslationContext, mkcmd


class _InspectObjectTaskBase(LightTask[None], ABC):
	__base_tr = TranslationContext('task.db_inspect_object_base')

	@classmethod
	def _gt_backup_id(cls, backup_id: int, hashtag: bool) -> RTextBase:
		t_bid = RText(f'{"#" if hashtag else ""}{backup_id}', TextColors.backup_id)
		return (
			t_bid.copy()
			.h(cls.__base_tr.tr('hover.backup_id', t_bid))
			.c(RAction.run_command, mkcmd(f'database inspect backup {backup_id}'))
		)

	@classmethod
	def _gt_fileset_id(cls, fileset_id: int) -> RTextBase:
		t_fsid = RText(str(fileset_id), TextColors.backup_id)
		return (
			t_fsid.copy()
			.h(cls.__base_tr.tr('hover.fileset_id', t_fsid))
			.c(RAction.run_command, mkcmd(f'database inspect fileset {fileset_id}'))
		)

	@classmethod
	def _gt_file_name(cls, file_path: str) -> RTextBase:
		return TextComponents.file_name(Path(file_path))

	@classmethod
	def _gt_blob_hash(cls, blob_hash: str, shorten_hash: bool = False) -> RTextBase:
		return (
			RText(blob_hash[:16] if shorten_hash else blob_hash, RColor.light_purple)
			.h(cls.__base_tr.tr('hover.blob_hash', RText(blob_hash, RColor.light_purple)))
			.c(RAction.run_command, mkcmd(f'database inspect blob {blob_hash}'))
		)

	@classmethod
	def _jsonfy(cls, obj: Any) -> RTextBase:
		return RText(json.dumps(obj, ensure_ascii=False))


class InspectBackupTask(_InspectObjectTaskBase):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	def id(self) -> str:
		return 'db_inspect_backup'

	def run(self) -> None:
		backup = GetBackupAction(self.backup_id, with_files=True).run()
		self.reply(TextComponents.title(self.tr('title', self._gt_backup_id(backup.id, True))))

		self.reply_tr('id', self._gt_backup_id(backup.id, False))
		self.reply_tr('timestamp', TextComponents.number(backup.timestamp_ns), TextComponents.date(backup.timestamp_ns))
		self.reply_tr('creator', self._jsonfy(str(backup.creator)), TextComponents.operator(backup.creator))

		comment = self._jsonfy(backup.comment)
		t_comment = TextComponents.backup_comment(backup.comment)
		if isinstance(t_comment, RTextMCDRTranslation):
			self.reply_tr('comment.translated', comment, t_comment)
		else:
			self.reply_tr('comment.regular', comment)

		self.reply_tr('targets', RTextBase.join(', ', [RText(t, TextColors.file) for t in backup.targets]))
		self.reply_tr('tags', self._jsonfy(backup.tags.to_dict()))
		self.reply_tr('raw_size', RText(backup.raw_size, TextColors.byte_count), TextComponents.file_size(backup.raw_size))
		self.reply_tr('stored_size', RText(backup.stored_size, TextColors.byte_count), TextComponents.file_size(backup.stored_size))
		self.reply_tr('file_count.all', TextComponents.number(len(backup.files)))

		def reply_count(key: str, func: Callable[[FileInfo], bool]):
			type_count = TextComponents.number(sum(1 for _ in filter(func, backup.files)))
			self.reply(RTextList(RText('- ', RColor.gray), self.tr(key, type_count)))
		reply_count('file_count.file', FileInfo.is_file)
		reply_count('file_count.directory', FileInfo.is_dir)
		reply_count('file_count.symlink', FileInfo.is_link)


class _InspectFileTaskBase(_InspectObjectTaskBase, ABC):
	def __init__(self, source: CommandSource, file_path: str):
		super().__init__(source)
		self.file_path = Path(file_path).as_posix()

	@property
	def id(self) -> str:
		return 'db_inspect_file'

	@abstractmethod
	def _get_file(self) -> FileInfo:
		raise NotImplementedError()

	def run(self) -> None:
		file = self._get_file()
		self.reply(TextComponents.title(self.tr('title', self._gt_fileset_id(file.fileset_id), self._gt_file_name(file.path))))

		self.reply_tr('fileset_id', self._gt_fileset_id(file.fileset_id))
		self.reply_tr('path', RText(file.path, TextColors.file))
		self.reply_tr('mode', TextComponents.number(file.mode), TextComponents.file_mode(file.mode))
		if file.content is not None:
			self.reply_tr('content', self._jsonfy(file.content_str))
		if file.blob is not None:
			self.reply_tr('blob.hash', self._gt_blob_hash(file.blob.hash))
			self.reply_tr('blob.compress', file.blob.compress.name)
			self.reply_tr('blob.raw_size', RText(file.blob.raw_size, TextColors.byte_count), TextComponents.file_size(file.blob.raw_size))
			self.reply_tr('blob.stored_size', RText(file.blob.stored_size, TextColors.byte_count), TextComponents.file_size(file.blob.stored_size))

		if file.uid is not None:
			if (uid_name := platform_utils.uid_to_name(file.uid)) is not None:
				self.reply_tr('uid.full', TextComponents.number(file.uid), uid_name)
			else:
				self.reply_tr('uid.simple', TextComponents.number(file.uid))
		if file.gid is not None:
			if (gid_name := platform_utils.gid_to_name(file.gid)) is not None:
				self.reply_tr('gid.full', TextComponents.number(file.gid), gid_name)
			else:
				self.reply_tr('gid.simple', TextComponents.number(file.gid))

		if file.mtime_ns is not None:
			self.reply_tr('mtime', TextComponents.number(file.mtime_ns), TextComponents.date(file.mtime_ns))


class InspectBackupFileTask(_InspectFileTaskBase):
	def __init__(self, source: CommandSource, backup_id: int, file_path: str):
		super().__init__(source, file_path)
		self.backup_id = backup_id

	def _get_file(self) -> FileInfo:
		return GetBackupFileAction(self.backup_id, self.file_path).run()


class InspectFilesetFileTask(_InspectFileTaskBase):
	def __init__(self, source: CommandSource, fileset_id: int, file_path: str):
		super().__init__(source, file_path)
		self.fileset_id = fileset_id

	def _get_file(self) -> FileInfo:
		return GetFilesetFileAction(self.fileset_id, self.file_path).run()


class InspectFilesetTask(_InspectObjectTaskBase):
	def __init__(self, source: CommandSource, fileset_id: int):
		super().__init__(source)
		self.fileset_id = fileset_id

	@property
	def id(self) -> str:
		return 'db_inspect_fileset'

	def run(self) -> None:
		fileset = GetFilesetAction(self.fileset_id, count_backups=True).run()
		self.reply(TextComponents.title(self.tr('title', self._gt_fileset_id(fileset.id))))

		self.reply_tr('id', self._gt_fileset_id(fileset.id))
		self.reply_tr('kind', TextComponents.fileset_kind(fileset))
		self.reply_tr('file_object_count', TextComponents.number(fileset.file_object_count))

		def bdk(tr_key: str) -> str:  # base-delta key
			return tr_key + '.' + ('base' if fileset.is_base else 'delta')

		self.reply_tr(bdk('file_count'), TextComponents.number(fileset.file_count))
		self.reply_tr(bdk('raw_size'), RText(fileset.raw_size, TextColors.byte_count), TextComponents.file_size(fileset.raw_size))
		self.reply_tr(bdk('stored_size'), RText(fileset.stored_size, TextColors.byte_count), TextComponents.file_size(fileset.stored_size))

		self.reply_tr('used_by', TextComponents.number(fileset.backup_count))


class InspectBlobTask(_InspectObjectTaskBase):
	def __init__(self, source: CommandSource, blob_hash: str):
		super().__init__(source)
		self.blob_hash = blob_hash

	@property
	def id(self) -> str:
		return 'db_inspect_blob'

	def run(self) -> None:
		blob = GetBlobByHashPrefixAction(self.blob_hash, count_files=True).run()
		self.reply(TextComponents.title(self.tr('title', self._gt_blob_hash(blob.hash, shorten_hash=True))))

		self.reply_tr('hash', self._gt_blob_hash(blob.hash))
		self.reply_tr('compress', blob.compress.name)
		self.reply_tr('raw_size', RText(blob.raw_size, TextColors.byte_count), TextComponents.file_size(blob.raw_size))
		self.reply_tr('stored_size', RText(blob.stored_size, TextColors.byte_count), TextComponents.file_size(blob.stored_size))

		self.reply_tr('used_by', TextComponents.number(blob.file_count))
