from pathlib import Path

from prime_backup.types.common import PathLike


def is_relative_to(child: Path, parent: PathLike) -> bool:
	if hasattr(child, 'is_relative_to'):  # python3.9+
		return child.is_relative_to(parent)
	else:
		try:
			child.relative_to(parent)
		except ValueError:
			return False
		else:
			return True
