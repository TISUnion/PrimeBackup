import contextlib
import enum
import gzip
import io
import lzma
import shutil
from abc import abstractmethod, ABC
from typing import BinaryIO, Union, ContextManager, NamedTuple

import lz4.frame
import zstandard
from typing_extensions import Protocol

from xbackup.types import PathLike


# noinspection PyAbstractClass
class ByPassReader(io.BytesIO):
	def __init__(self, file_obj, do_hash: bool):
		super().__init__()
		self.file_obj: io.BytesIO = file_obj
		from xbackup.utils import hash_utils
		self.hasher = hash_utils.create_hasher() if do_hash else None
		self.read_len = 0

	def read(self, *args, **kwargs):
		data = self.file_obj.read(*args, **kwargs)
		self.read_len += len(data)
		if self.hasher is not None:
			self.hasher.update(data)
		return data

	def readall(self):
		raise NotImplementedError()

	def readinto(self, b: Union[bytearray, memoryview]):
		n = self.file_obj.readinto(b)
		if n:
			self.read_len += n
			if self.hasher is not None:
				self.hasher.update(b[:n])
		return n

	def get_read_len(self) -> int:
		return self.read_len

	def get_hash(self) -> str:
		return self.hasher.hexdigest() if self.hasher is not None else ''

	def __getattribute__(self, item: str):
		if item in (
				'read', 'readall', 'readinto',
				'get_hash', 'get_read_len', 'file_obj', 'hasher', 'read_len',
		):
			return object.__getattribute__(self, item)
		else:
			return self.file_obj.__getattribute__(item)


class Compressor(ABC):
	class CopyCompressResult(NamedTuple):
		size: int
		hash: str

	@classmethod
	def create(cls, method: Union[str, 'CompressMethod']) -> 'Compressor':
		if not isinstance(method, CompressMethod):
			if method in CompressMethod.__members__:
				method = CompressMethod[method]
			else:
				raise ValueError(f'Unknown compression method: {method}')
		return method.value()

	@classmethod
	def get_method(cls) -> 'CompressMethod':
		return CompressMethod(cls)

	@classmethod
	def get_name(cls) -> str:
		return cls.get_method().name

	def copy_compressed(self, source_path: PathLike, dest_path: PathLike, *, calc_hash: bool = False) -> CopyCompressResult:
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			reader = ByPassReader(f_in, calc_hash)
			self._copy_compressed(reader, f_out)
			return self.CopyCompressResult(reader.get_read_len(), reader.get_hash())

	def copy_decompressed(self, source_path: PathLike, dest_path: PathLike):
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			self._copy_decompressed(f_in, f_out)

	@contextlib.contextmanager
	def open_compressed(self, target_path: PathLike) -> ContextManager[BinaryIO]:
		with open(target_path, 'wb') as f:
			with self.compress_stream(f) as f_compressed:
				yield f_compressed

	@contextlib.contextmanager
	def open_decompressed(self, source_path: PathLike) -> ContextManager[BinaryIO]:
		with open(source_path, 'rb') as f:
			with self.decompress_stream(f) as f_decompressed:
				yield f_decompressed

	@abstractmethod
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		...

	@abstractmethod
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		...

	@abstractmethod
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		...

	@abstractmethod
	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		...


class PlainCompressor(Compressor):
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		shutil.copyfileobj(f_in, f_out)

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		yield f_out

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		yield f_in


class _GzipLikeLibrary(Protocol):
	def open(self, file_obj: BinaryIO, mode: str) -> BinaryIO:
		...


class _GzipLikeCompressorBase(Compressor):
	_lib: _GzipLikeLibrary

	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self.compress_stream(f_out) as compressed_out:
			shutil.copyfileobj(f_in, compressed_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self.decompress_stream(f_in) as compressed_in:
			shutil.copyfileobj(compressed_in, f_out)

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib.open(f_out, 'wb') as compressed_out:
			yield compressed_out

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib.open(f_in, 'rb') as compressed_in:
			yield compressed_in


class GzipCompressor(_GzipLikeCompressorBase):
	_lib = gzip


class LzmaCompressor(_GzipLikeCompressorBase):
	_lib = lzma


class ZstdCompressor(_GzipLikeCompressorBase):
	_lib = zstandard


class Lz4Compressor(_GzipLikeCompressorBase):
	_lib = lz4.frame


class CompressMethod(enum.Enum):
	plain = PlainCompressor
	gzip = GzipCompressor
	lzma = LzmaCompressor
	zstd = ZstdCompressor
	lz4 = Lz4Compressor
