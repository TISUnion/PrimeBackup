import os
import shutil
from pathlib import Path


def copy_file_fast(src_path: Path, dst_path: Path):
	if callable(getattr(os, 'copy_file_range', None)):
		with open(src_path, 'rb') as f_src, open(dst_path, 'wb+') as f_dst:
			while os.copy_file_range(f_src.fileno(), f_dst.fileno(), 2 ** 30):
				pass
	else:
		shutil.copyfile(src_path, dst_path, follow_symlinks=False)
