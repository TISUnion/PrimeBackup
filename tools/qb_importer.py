#!/usr/bin/env python3
import argparse
import contextlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import List

args: argparse.Namespace


def make_pb_meta(creator: str, timestamp: float, comment: str, targets: List[str]) -> str:
	return json.dumps({
		'creator': f'player:{creator}',
		'comment': comment,
		'timestamp_ns': int(timestamp * 1e9),
		'targets': list(targets),
		'tags': {},
	}, ensure_ascii=False)


def import_plain(slot_path: Path, timestamp: float, comment: str):
	targets = []
	for name in os.listdir(slot_path):
		if name != 'info.json':
			targets.append(name)
	print(f'Collected target at {slot_path}: {targets}')

	if os.path.isdir(args.temp):
		shutil.rmtree(args.temp)

	with contextlib.ExitStack() as es:
		os.mkdir(args.temp)
		es.callback(lambda: shutil.rmtree(args.temp, ignore_errors=True))
		print('Creating temp tar file for PB import')

		temp_tar_path = Path(args.temp) / (slot_path.name + '.tar')
		with tarfile.TarFile(temp_tar_path, 'w') as tar:
			for target in targets:
				tar.add(slot_path / target, arcname=target)

		cmd_args = [
			sys.executable, args.executable,
			'--db', args.db,
			'import', str(temp_tar_path),
			'--format', 'tar',
			'--meta-override', make_pb_meta(args.creator, timestamp, comment, targets),
		]
		print(f'Importing temp tar file to PB for {slot_path}')
		print(f'Cmd args: {cmd_args}')
		subprocess.check_call(cmd_args)
		print('Import ok')
		temp_tar_path.unlink()


def import_file(slot_path: Path, backup_format: str, timestamp: float, comment: str):
	ext = {
		'tar': '.tar',
		'tar_gz': '.tar.gz',
		'tar_xz': '.tar.xz',
	}.get(backup_format)
	if ext is None:
		raise ValueError(f'Invalid backup format {backup_format}')

	backup_file_path = slot_path / ('backup' + ext)
	print(f'Processing backup file {backup_file_path}')
	if not backup_file_path.is_file():
		raise FileNotFoundError(f'Backup file {backup_file_path} is not a file')

	tar_mode = {
		'tar': 'r:',
		'tar_gz': 'r:gz',
		'tar_xz': 'r:xz',
	}[backup_format]
	with tarfile.open(backup_file_path, tar_mode) as tar:
		targets = [name for name in tar.getnames() if '/' not in name]
	print(f'Collected target from {backup_file_path}: {targets}')

	cmd_args = [
		sys.executable, args.executable,
		'--db', args.db,
		'import', str(backup_file_path),
		'--format', backup_format,
		'--meta-override', make_pb_meta(args.creator, timestamp, comment, targets),
	]
	print(f'Importing backup file to PB for {slot_path}')
	print(f'Cmd args: {cmd_args}')
	subprocess.check_call(cmd_args)
	print('Import ok')


def import_slot(slot_path: Path) -> bool:
	try:
		with open(slot_path / 'info.json', 'r', encoding='utf8') as f:
			info: dict = json.load(f)
	except (ValueError, OSError):
		print(f'Reading info.json failed, skipped slot {slot_path}')
		return False

	try:
		timestamp = info.get('time_stamp', None)
		if timestamp is None:
			timestamp = time.mktime(time.strptime('%Y-%m-%d %H:%M:%S', info.get('time', '')))
		comment: str = info.get('comment', '')
		backup_format: str = info.get('backup_format', '')
	except Exception:
		print(f'Parsing info.json failed, slot {slot_path}')
		return False

	if backup_format in ['', 'plain']:
		import_plain(slot_path, timestamp, comment)
	else:
		import_file(slot_path, backup_format, timestamp, comment)

	return True


HELP_MESSAGE = '''
A tool to import backups from QuickBackupMulti to PrimeBackup

Example usages:

  {self} --help
  {self} -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files -c Steve
  {self} -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files --slot 1
'''.strip().format(self=sys.argv[0])


class ArgFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
	pass


def main():
	parser = argparse.ArgumentParser(description=HELP_MESSAGE, formatter_class=ArgFormatter)
	parser.add_argument('-i', '--input', required=True, help='Path to the QuickBackupMulti backup file root, e.g. /path/to/qb_multi')
	parser.add_argument('-x', '--executable', required=True, help='Path to the PrimeBackup plugin file')
	parser.add_argument('-d', '--db', required=True, help='Path to the PrimeBackup file root that contains the database file and so on, e.g. /path/to/pb_files')
	parser.add_argument('-t', '--temp', default='./qb_importer_temp', help='Path for placing temp files for import')
	parser.add_argument('-c', '--creator', default='QuickBackupM', help='Creator of the imported backup')
	parser.add_argument('-s', '--slot', type=int, help='Specified the slot number to import. If not provided, import all slots')

	global args
	args = parser.parse_args()
	print(args)

	if not os.path.isfile(args.executable):
		raise FileNotFoundError(f'PrimeBackup plugin file {args.executable} is not a valid file')

	t = time.time()
	imported_cnt = 0
	for slot_dir_name in os.listdir(args.input):
		slot_path = Path(args.input) / slot_dir_name
		if not slot_path.is_dir():
			print(f'Skipping non-dir slot at {slot_path}')
			continue

		if args.slot is not None and slot_path.name != f'slot{args.slot}':
			print(f'Skipping slot at {slot_path}')
			continue

		print(f'Importing slot {slot_path}')
		ok = import_slot(slot_path)
		if ok:
			imported_cnt += 1

	print(f'All done, imported {imported_cnt} slots, cost {time.time() - t:.1f}s in total')


if __name__ == '__main__':
	main()
