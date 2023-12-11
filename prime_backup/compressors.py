import contextlib
import enum
import gzip
import lzma
import shutil
from abc import abstractmethod, ABC
from typing import BinaryIO, Union, ContextManager, NamedTuple, Tuple

from typing_extensions import Protocol

from prime_backup.types.common import PathLike
from prime_backup.utils.bypass_io import BypassReader, BypassWriter


class Compressor(ABC):
	class CopyCompressResult(NamedTuple):
		read_size: int
		read_hash: str
		write_size: int

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
		"""
		source --[compress]--> destination
		"""
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			reader = BypassReader(f_in, calc_hash=calc_hash)
			writer = BypassWriter(f_out)
			self._copy_compressed(reader, writer)
			return self.CopyCompressResult(reader.get_read_len(), reader.get_hash(), writer.get_write_len())

	def copy_decompressed(self, source_path: PathLike, dest_path: PathLike):
		"""
		source --[decompress]--> destination
		"""
		with open(source_path, 'rb') as f_in, open(dest_path, 'wb') as f_out:
			self._copy_decompressed(f_in, f_out)

	@contextlib.contextmanager
	def open_compressed(self, target_path: PathLike) -> ContextManager[BinaryIO]:
		"""
		(writer) --[compress]--> target_path
		"""
		with open(target_path, 'wb') as f:
			with self.compress_stream(f) as f_compressed:
				yield f_compressed

	@contextlib.contextmanager
	def open_compressed_bypassed(self, target_path: PathLike) -> ContextManager[Tuple[BypassWriter, BinaryIO]]:
		"""
		(writer) --[compress]--> target_path
		                      ^- bypassed
		"""
		with open(target_path, 'wb') as f:
			writer = BypassWriter(f)
			with self.compress_stream(writer) as f_compressed:
				yield writer, f_compressed

	@contextlib.contextmanager
	def open_decompressed(self, source_path: PathLike) -> ContextManager[BinaryIO]:
		"""
		source_path --[decompress]--> (reader)
		"""
		with open(source_path, 'rb') as f:
			with self.decompress_stream(f) as f_decompressed:
				yield f_decompressed

	@contextlib.contextmanager
	def open_decompressed_bypassed(self, source_path: PathLike) -> ContextManager[Tuple[BypassReader, BinaryIO]]:
		"""
		source_path --[decompress]--> (reader)
		             ^- bypassed
		"""
		with open(source_path, 'rb') as f:
			reader = BypassReader(f, calc_hash=False)  # it's meaningless to calc hash on the compressed file
			with self.decompress_stream(reader) as f_decompressed:
				yield reader, f_decompressed

	@abstractmethod
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		"""
		Open a stream from compressing write
		"""
		...

	@abstractmethod
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		"""
		Open a stream from decompressing read
		"""
		...

	@abstractmethod
	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		"""
		shutil.copyfileobj on: f_in --[compress]--> f_out
		"""
		...

	@abstractmethod
	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		"""
		shutil.copyfileobj on: f_in --[decompress]--> f_out
		"""
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


class _GzipLikeCompressorBase(Compressor, ABC):
	@classmethod
	def _lib(cls) -> _GzipLikeLibrary:
		...

	def _copy_compressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self.compress_stream(f_out) as compressed_out:
			shutil.copyfileobj(f_in, compressed_out)

	def _copy_decompressed(self, f_in: BinaryIO, f_out: BinaryIO):
		with self.decompress_stream(f_in) as compressed_in:
			shutil.copyfileobj(compressed_in, f_out)

	@contextlib.contextmanager
	def compress_stream(self, f_out: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib().open(f_out, 'wb') as compressed_out:
			yield compressed_out

	@contextlib.contextmanager
	def decompress_stream(self, f_in: BinaryIO) -> ContextManager[BinaryIO]:
		with self._lib().open(f_in, 'rb') as compressed_in:
			yield compressed_in


class GzipCompressor(_GzipLikeCompressorBase):
	@classmethod
	def _lib(cls):
		return gzip


class LzmaCompressor(_GzipLikeCompressorBase):
	@classmethod
	def _lib(cls):
		return lzma


class ZstdCompressor(_GzipLikeCompressorBase):
	@classmethod
	def _lib(cls):
		import zstandard
		return zstandard


class Lz4Compressor(_GzipLikeCompressorBase):
	@classmethod
	def _lib(cls):
		# noinspection PyPackageRequirements
		import lz4.frame
		return lz4.frame


class CompressMethod(enum.Enum):
	plain = PlainCompressor
	gzip = GzipCompressor
	lzma = LzmaCompressor
	zstd = ZstdCompressor
	lz4 = Lz4Compressor
