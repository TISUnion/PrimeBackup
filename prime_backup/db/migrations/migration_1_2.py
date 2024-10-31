from typing import List, Tuple

from sqlalchemy import text

from prime_backup.db.migrations import MigrationImplBase


class MigrationImpl1To2(MigrationImplBase):
	def migrate(self):
		from prime_backup.types.backup_tags import BackupTagName

		# TODO: verify this works
		src_tag = 'pre_restore_backup'
		dst_tag = BackupTagName.temporary.name
		changes: List[Tuple[int, dict]] = []

		for backup in self.session.execute(text('SELECT * FROM backup')):
			tags = dict(backup.tags)
			if src_tag in tags:
				tags[dst_tag] = tags.pop(src_tag)
				changes.append((backup.id, tags))

		for backup_id, tags in changes:
			self.session.execute(text('UPDATE backup SET tags = :tags WHERE id = :backup_id').bindparams(backup_id=backup_id, tags=tags))
			self.logger.info('Renaming tag {!r} to {!r} for backup #{}, new tags: {}'.format(
				src_tag, dst_tag, backup_id, tags,
			))
