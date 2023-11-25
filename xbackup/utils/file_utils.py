import shutil
from pathlib import Path


def copy_file_fast(src_path: Path, dst_path: Path):
	shutil.copyfile(src_path, dst_path, follow_symlinks=False)
