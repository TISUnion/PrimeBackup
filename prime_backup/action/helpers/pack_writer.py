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

	def append(self, data: bytes) -> PackEntryLocation:
		return self.append_reader(_BytesReader(data), len(data))

	def append_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		offset = self.pack.size
		remaining = size
		while remaining > 0:
			buf = reader.read(min(1024 * 1024, remaining))
			if not buf:
				raise EOFError('reader exhausted with {} bytes remaining'.format(remaining))
			self.file.write(buf)
			remaining -= len(buf)
		self.file.flush()

		self.pack.size += size
		self.pack.count += 1
		self.pack.live_size += size
		self.pack.live_count += 1

		return PackEntryLocation(self.pack.id, self.pack.name, offset)

	def append_bytes(self, data: bytes) -> PackEntryLocation:
		self.file.write(data)
		self.file.flush()

		offset = self.pack.size
		self.pack.size += len(data)
		self.pack.count += 1
		self.pack.live_size += len(data)
		self.pack.live_count += 1

		return PackEntryLocation(self.pack.id, self.pack.name, offset)

	def close(self):
		self.file.close()


class _BytesReader:
	def __init__(self, data: bytes):
		self.__data = data
		self.__offset = 0

	def read(self, size: int = -1) -> bytes:
		if self.__offset >= len(self.__data):
			return b''
		if size is None or size < 0:
			size = len(self.__data) - self.__offset
		data = self.__data[self.__offset:self.__offset + size]
		self.__offset += len(data)
		return data


class PackWriter:
	def __init__(self, session: DbSession):
		from prime_backup import logger
		self.session = session
		self.logger = logger.get()

		self.__lock = threading.Lock()
		self.__active: Optional[_ActivePack] = None
		self.__new_pack_names: List[str] = []
		self.__created_pack_size = 0

	def get_rollback_paths(self) -> List[Path]:
		return [pack_utils.get_pack_path(name) for name in self.__new_pack_names]

	def get_new_pack_names(self) -> List[str]:
		return list(self.__new_pack_names)

	def get_created_pack_summary(self) -> PackChangeSummary:
		return PackChangeSummary(new_size=self.__created_pack_size)

	def write_entry(self, data: bytes) -> PackEntryLocation:
		if len(data) > pack_constants.PACK_MAX_SIZE:
			return self.__write_dedicated(data)
		return self.__write_active(data)

	def write_pack_entry(self, data: bytes) -> PackEntryLocation:
		return self.write_entry(data)

	def write_entry_from_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		if size < 0:
			raise ValueError('negative entry size {}'.format(size))
		if size > pack_constants.PACK_MAX_SIZE:
			return self.__write_dedicated_reader(reader, size)
		return self.__write_active_reader(reader, size)

	def __write_active(self, data: bytes) -> PackEntryLocation:
		with self.__lock:
			if self.__active is None or self.__should_rotate_no_lock(len(data)):
				self.__rotate_no_lock()
			assert self.__active is not None
			result = self.__active.append_bytes(data)
			self.__created_pack_size += len(data)
			return result

	def __write_active_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		with self.__lock:
			if self.__active is None or self.__should_rotate_no_lock(size):
				self.__rotate_no_lock()
			assert self.__active is not None
			result = self.__active.append_reader(reader, size)
			self.__created_pack_size += size
			return result

	def __write_dedicated(self, data: bytes) -> PackEntryLocation:
		with self.__lock:
			dedicated_pack = self.__create_new_pack_no_lock()
			result = dedicated_pack.append_bytes(data)
			self.__created_pack_size += len(data)
			dedicated_pack.close()
		self.logger.debug(f'Wrote dedicated pack id={dedicated_pack.pack.id} name={dedicated_pack.pack.name} size={len(data)}')
		return result

	def __write_dedicated_reader(self, reader: SupportsReadBytes, size: int) -> PackEntryLocation:
		with self.__lock:
			dedicated_pack = self.__create_new_pack_no_lock()
			result = dedicated_pack.append_reader(reader, size)
			self.__created_pack_size += size
			dedicated_pack.close()
		self.logger.debug(f'Wrote dedicated pack id={dedicated_pack.pack.id} name={dedicated_pack.pack.name} size={size}')
		return result

	def __should_rotate_no_lock(self, next_entry_size: int) -> bool:
		assert self.__active is not None
		if self.__active.pack.count >= pack_constants.PACK_MAX_COUNT:
			return True
		return self.__active.pack.size > 0 and self.__active.pack.size + next_entry_size > pack_constants.PACK_MAX_SIZE

	def close(self):
		with self.__lock:
			self.__close_no_lock()

	def __close_no_lock(self):
		if self.__active is not None:
			self.__active.close()
		self.__active = None

	def __create_new_pack_no_lock(self) -> _ActivePack:
		pack_utils.prepare_pack_store()
		for _ in range(100):
			new_pack_name = pack_utils.generate_pack_name()
			if self.session.get_pack_by_name_opt(new_pack_name) is not None:  # Extremely unlikely
				self.logger.warning(f'Generated pack UUID {new_pack_name} already exists in DB')
				continue
			break
		else:
			raise RuntimeError('Failed to generate a unique pack UUID')

		new_pack = self.session.create_and_add_pack(
			name=new_pack_name,
			size=0,
			count=0,
			live_size=0,
			live_count=0,
		)
		self.session.flush()  # creates pack.id

		pack_path = pack_utils.get_pack_path(new_pack_name)
		pack_path.parent.mkdir(parents=True, exist_ok=True)
		fh = open(pack_path, 'wb')
		self.__new_pack_names.append(new_pack_name)
		return _ActivePack(new_pack, fh)

	def __rotate_no_lock(self):
		self.__close_no_lock()

		new_pack = self.__create_new_pack_no_lock()
		self.__active = new_pack
		self.logger.debug(f'Opened new active pack id={new_pack.pack.id} name={new_pack.pack.name}')
