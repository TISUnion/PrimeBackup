from pathlib import Path
from typing import List, Optional

from mcdreforged.api.utils import Serializable

from prime_backup.compressors import CompressMethod
from prime_backup.types.hash_method import HashMethod


class BackupConfig(Serializable):
	source_root: str = './server'
	targets: List[str] = [
		'world',
	]
	ignored_files: List[str] = [
		'session.lock',
	]
	follow_target_symlink: bool = False
	hash_method: HashMethod = HashMethod.xxh128
	compress_method: CompressMethod = CompressMethod.zstd
	compress_threshold: int = 64

	def get_compress_method_from_size(self, file_size: int, *, compress_method_override: Optional[CompressMethod] = None) -> CompressMethod:
		if file_size < self.compress_threshold:
			return CompressMethod.plain
		else:
			if compress_method_override is not None:
				return compress_method_override
			else:
				return self.compress_method

	def is_file_ignore(self, full_path: Path) -> bool:
		"""
		Apply to not only files
		"""
		# TODO: better rule?
		name = full_path.name
		for item in self.ignored_files:
			if len(item) > 0:
				if item[0] == '*' and name.endswith(item[1:]):
					return True
				if item[-1] == '*' and name.startswith(item[:-1]):
					return True
				if name == item:
					return True
		return False
