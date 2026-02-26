import functools
from typing import Iterable, Tuple

import pathspec


@functools.lru_cache(maxsize=32)
def __compile_gitignore_spec(lines: Tuple[str, ...]) -> pathspec.GitIgnoreSpec:
	return pathspec.GitIgnoreSpec.from_lines(lines)


def compile_gitignore_spec(lines: Iterable[str]) -> pathspec.GitIgnoreSpec:
	return __compile_gitignore_spec(tuple(lines))
