import dataclasses
import threading
from pathlib import Path
from typing import BinaryIO, Optional, List

from prime_backup.constants import pack_constants
from prime_backup.db import schema
from prime_backup.db.session import DbSession
from prime_backup.types.pack_info import PackEntryLocation, PackChangeSummary
from prime_backup.utils import pack_utils
from prime_backup.utils.io_types import SupportsReadBytes


@dataclasses.dataclass(frozen=True)
class _ActivePack:
	pack: schema.Pack
	file: BinaryIO

	def append_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		offset = self.pack.size
		remaining = size
		while remaining > 0:
			buf = reader.read(min(1024 * 1024, remaining))
			if not buf:
				raise EOFError('reader exhausted with {} bytes remaining'.format(remaining))
			self.file.write(buf)
			remaining -= len(buf)

		self.pack.size += size
		self.pack.entry_count += 1
		self.pack.live_size += size
		self.pack.live_entry_count += 1

		return PackEntryLocation(self.pack.id, offset)

	def append_bytes(self, data: bytes) -> PackEntryLocation:
		self.file.write(data)

		offset = self.pack.size
		self.pack.size += len(data)
		self.pack.entry_count += 1
		self.pack.live_size += len(data)
		self.pack.live_entry_count += 1

		return PackEntryLocation(self.pack.id, offset)

	def close(self):
		self.file.flush()
		self.file.close()


class PackWriter:
	def __init__(self, session: DbSession):
		from prime_backup import logger
		self.session = session
		self.logger = logger.get()

		self.__lock = threading.Lock()
		self.__active: Optional[_ActivePack] = None
		self.__new_pack_paths: List[Path] = []
		self.__created_pack_size = 0

	def get_rollback_paths(self) -> List[Path]:
		return list(self.__new_pack_paths)

	def get_created_pack_summary(self) -> PackChangeSummary:
		return PackChangeSummary(created_pack_count=len(self.__new_pack_paths), new_size=self.__created_pack_size)

	def write_entry(self, data: bytes) -> PackEntryLocation:
		if self.__should_write_dedicated(len(data)):
			return self.__write_dedicated(data)
		return self.__write_active(data)

	def write_entry_from_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		if size < 0:
			raise ValueError('negative entry size {}'.format(size))
		if self.__should_write_dedicated(size):
			return self.__write_dedicated_reader(reader, size)
		return self.__write_active_reader(reader, size)

	@staticmethod
	def __should_write_dedicated(size: int) -> bool:
		return size >= pack_constants.PACK_DEDICATED_ENTRY_MIN_SIZE

	def __write_active(self, data: bytes) -> PackEntryLocation:
		with self.__lock:
			active = self.__get_active_for_write_no_lock()
			result = active.append_bytes(data)
			self.__created_pack_size += len(data)
			return result

	def __write_active_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		with self.__lock:
			active = self.__get_active_for_write_no_lock()
			result = active.append_reader(reader, size)
			self.__created_pack_size += size
			return result

	def __write_dedicated(self, data: bytes) -> PackEntryLocation:
		with self.__lock:
			dedicated_pack = self.__create_new_pack_no_lock()
			result = dedicated_pack.append_bytes(data)
			self.__created_pack_size += len(data)
			dedicated_pack.close()
		self.logger.debug(f'Wrote dedicated pack id={dedicated_pack.pack.id} file_name={pack_utils.get_pack_file_name(dedicated_pack.pack.id)} size={len(data)}')
		return result

	def __write_dedicated_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		with self.__lock:
			dedicated_pack = self.__create_new_pack_no_lock()
			result = dedicated_pack.append_reader(reader, size)
			self.__created_pack_size += size
			dedicated_pack.close()
		self.logger.debug(f'Wrote dedicated pack id={dedicated_pack.pack.id} file_name={pack_utils.get_pack_file_name(dedicated_pack.pack.id)} size={size}')
		return result

	def __get_active_for_write_no_lock(self) -> _ActivePack:
		if self.__active is None or self.__should_rotate_active_no_lock():
			self.__open_new_active_no_lock()
		assert self.__active is not None
		return self.__active

	def __should_rotate_active_no_lock(self) -> bool:
		assert self.__active is not None
		return (
			self.__active.pack.size >= pack_constants.PACK_MAX_SIZE or
			self.__active.pack.entry_count >= pack_constants.PACK_MAX_COUNT
		)

	def close(self):
		with self.__lock:
			self.__close_no_lock()

	def __close_no_lock(self):
		if self.__active is not None:
			self.__active.close()
		self.__active = None

	def __create_new_pack_no_lock(self) -> _ActivePack:
		pack_utils.prepare_pack_store()
		new_pack = self.session.create_and_add_pack(
			size=0,
			entry_count=0,
			live_size=0,
			live_entry_count=0,
		)
		self.session.flush()  # creates pack.id

		pack_path = pack_utils.get_pack_path(new_pack.id)
		pack_path.parent.mkdir(parents=True, exist_ok=True)
		fh = open(pack_path, 'wb')
		self.__new_pack_paths.append(pack_path)
		return _ActivePack(new_pack, fh)

	def __open_new_active_no_lock(self):
		self.__close_no_lock()

		new_pack = self.__create_new_pack_no_lock()
		self.__active = new_pack
		self.logger.debug(f'Opened new active pack id={new_pack.pack.id} file_name={pack_utils.get_pack_file_name(new_pack.pack.id)}')
