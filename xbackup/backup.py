from pathlib import Path

'''
xbackup_files/
	blobs/
		<hash[:2]>/
			<hash>
	backups/
	output/
	xbackup.db
'''


class Blob:
	pass


class Backup:
	def __init__(self, base_dir: Path):
		self.base_dir = base_dir

	def add_file(self, file_path: Path):
		file_name = file_path.name
		related_path = file_path.relative_to(self.base_dir)

	# TODO: compress
	def copy_file(self, src_path: Path, target: Path):
		pass


