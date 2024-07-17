from typing import List, Optional

from mcdreforged.api.utils import Serializable

from prime_backup.compressors import CompressMethod
from prime_backup.types.hash_method import HashMethod


class BackupConfig(Serializable):
	source_root: str = './server'
	source_root_use_mcdr_working_directory: bool = False
	targets: List[str] = [
		'world',
	]
	ignored_files: List[str] = []  # deprecated
	ignore_patterns: List[str] = [
		'**/session.lock',
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

	def is_file_ignore_by_deprecated_ignored_files(self, file_name: str) -> bool:
		for item in self.ignored_files:
			if len(item) > 0:
				if item[0] == '*' and file_name.endswith(item[1:]):
					return True
				if item[-1] == '*' and file_name.startswith(item[:-1]):
					return True
				if file_name == item:
					return True
		return False
