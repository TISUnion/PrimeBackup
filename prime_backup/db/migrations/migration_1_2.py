import json
from typing import List, Tuple

from sqlalchemy import text

from prime_backup.db.migrations import MigrationImplBase


class MigrationImpl1To2(MigrationImplBase):
	def migrate(self):
		src_tag = 'pre_restore_backup'
		dst_tag = 'temporary'
		changes: List[Tuple[int, str]] = []  # list of (backup_id, tags_json_str)

		for backup in self.session.execute(text('SELECT * FROM backup')):
			# noinspection PyProtectedMember
			tags_str: str = backup._mapping['tags']
			try:
				tags: dict = json.loads(tags_str)
			except ValueError:
				self.logger.error('Skipping invalid backup tags {!r}'.format(tags_str))
				continue
			if src_tag in tags:
				tags[dst_tag] = tags.pop(src_tag)
				changes.append((backup.id, json.dumps(tags)))

		for backup_id, tags_str in changes:
			self.session.execute(text('UPDATE backup SET tags = :tags WHERE id = :backup_id').bindparams(backup_id=backup_id, tags=tags_str))
			self.logger.info('Renaming tag {!r} to {!r} for backup #{}, new tags: {}'.format(
				src_tag, dst_tag, backup_id, tags_str,
			))
