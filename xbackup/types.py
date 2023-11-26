import os
from dataclasses import dataclass
from typing import Union, NamedTuple, Optional

from mcdreforged.api.all import *

from xbackup import constants
from xbackup.db import schema
from xbackup.utils import conversion_utils

PathLike = Union[str, bytes, os.PathLike]


class Operator(NamedTuple):
	type: str
	name: str

	@classmethod
	def xbackup(cls, what: str) -> 'Operator':
		return Operator(constants.PLUGIN_ID, what)

	@classmethod
	def player(cls, name: str) -> 'Operator':
		return Operator('player', name)

	@classmethod
	def console(cls) -> 'Operator':
		return Operator('console', '')

	@classmethod
	def of(cls, value: Union[str, CommandSource]) -> 'Operator':
		if isinstance(value, CommandSource):
			if value.is_player:
				# noinspection PyUnresolvedReferences
				return cls.player(value.player)
			elif value.is_console:
				return cls.console()
			else:
				return Operator('command_source', str(value))
		elif isinstance(value, str):
			if ':' in value:
				t, n = value.split(':', 1)
				return Operator(type=t, name=n)
			else:
				return Operator(type=value, name='')
		else:
			raise TypeError(value)

	def __str__(self):
		return f'{self.type}:{self.name}'


@dataclass
class BackupFilter:
	author: Optional[Operator] = None
	timestamp_lower: Optional[int] = None
	timestamp_upper: Optional[int] = None


class BackupInfo(NamedTuple):
	id: int
	timestamp_ns: int
	date: str
	author: Operator
	comment: str
	size: int  # actual uncompressed size

	@classmethod
	def of(cls, backup: schema.Backup) -> 'BackupInfo':
		"""
		Notes: should be inside a session
		"""
		size_sum = 0
		for file in backup.files:
			if file.blob_size is not None:
				size_sum += file.blob_size
		return BackupInfo(
			id=backup.id,
			timestamp_ns=backup.timestamp,
			date=conversion_utils.timestamp_to_local_date(backup.timestamp, decimal=False),
			author=Operator.of(backup.author),
			comment=backup.comment,
			size=size_sum,
		)

	def pretty_text(self, with_buttons: bool) -> RTextBase:
		from xbackup.utils.mcdr_utils import tr
		# TODO
		base_info = tr('pretty_backup.base', self.id, self.date, self.comment)
		return base_info
