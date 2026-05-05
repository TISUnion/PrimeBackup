from typing import List, Optional, TYPE_CHECKING

from mcdreforged.api.utils import Serializable

from prime_backup.compressors import CompressMethod
from prime_backup.types.chunk_method import ChunkMethod
from prime_backup.types.hash_method import HashMethod

if TYPE_CHECKING:
	import pathspec


class ChunkingRule(Serializable):
	algorithm: ChunkMethod
	file_size_threshold: int
	patterns: List[str] = []

	@property
	def patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.patterns)


class BackupConfig(Serializable):
	# Source
	source_root: str = './server'
	source_root_use_mcdr_working_directory: bool = False
	targets: List[str] = [
		'world',
	]

	# Strategy
	ignored_files: List[str] = []  # deprecated
	ignore_patterns: List[str] = [
		'**/session.lock',
	]
	retain_patterns: List[str] = []
	follow_target_symlink: bool = False
	reuse_stat_unchanged_file: bool = False
	creation_skip_missing_file: bool = False
	creation_skip_missing_file_patterns: List[str] = [
		'**',
	]
	mutating_file_patterns: List[str] = []

	# Chunking
	chunking_enabled: bool = False
	chunking_rules: List[ChunkingRule] = [
		ChunkingRule(
			algorithm=ChunkMethod.fastcdc_32k,
			file_size_threshold=100 * 1048576,
			patterns=[
				'**/*.db'
			],
		),
	]

	# Storage
	hash_method: HashMethod = HashMethod.blake3
	compress_method: CompressMethod = CompressMethod.zstd
	compress_threshold: int = 64

	# Advanced
	fileset_allocate_lookback_count: int = 2

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

	@property
	def targets_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.targets)

	@property
	def ignore_patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.ignore_patterns)

	@property
	def retain_patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.retain_patterns)

	@property
	def ignore_or_retained_patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec([
			*self.ignore_patterns,
			*self.retain_patterns,
		])

	@property
	def creation_skip_missing_file_patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.creation_skip_missing_file_patterns)

	@property
	def mutating_file_patterns_spec(self) -> 'pathspec.GitIgnoreSpec':
		from prime_backup.utils import pathspec_utils
		return pathspec_utils.compile_gitignore_spec(self.mutating_file_patterns)
