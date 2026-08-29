import json
import logging


class ConfigMigrator:
	def __init__(self, logger: logging.Logger):
		self.logger = logger

	def migrate(self, config: dict) -> bool:
		"""
		:return: if the config has been changed
		"""
		prev_state = json.dumps(config)

		# Migration starts
		self.__1_rename_pre_restore_backup_to_temporary(config)
		self.__2_move_restore_settings(config)
		# Migration ends

		return json.dumps(config) != prev_state

	def __1_rename_pre_restore_backup_to_temporary(self, config: dict):
		"""
		Change in v1.7.0
		"""
		prune_config = config.get('prune', None)
		if not isinstance(prune_config, dict):
			return

		src_key = 'pre_restore_backup'
		dst_key = 'temporary_backup'
		if src_key in prune_config and dst_key not in prune_config:
			prune_config[dst_key] = prune_config.pop(src_key)
			self.logger.info('Renamed prune config key {!r} -> {!r}'.format(src_key, dst_key))

	def __2_move_restore_settings(self, config: dict):
		for src_section_name, src_key, dst_key in [
			('command', 'backup_on_restore', 'create_pre_restore_backup'),
			('command', 'restore_countdown_sec', 'countdown_sec'),
		]:
			src_section = config.get(src_section_name)
			if not isinstance(src_section, dict) or src_key not in src_section:
				continue

			restore_config = config.setdefault('restore', {})
			if not isinstance(restore_config, dict):
				return

			value = src_section.pop(src_key)
			if dst_key not in restore_config:
				restore_config[dst_key] = value
				self.logger.info('Moved config key {!r} -> {!r}'.format(
					'{}.{}'.format(src_section_name, src_key),
					'restore.{}'.format(dst_key),
				))
			else:
				self.logger.info('Removed obsolete config key {!r}; {!r} is already set'.format(
					'{}.{}'.format(src_section_name, src_key),
					'restore.{}'.format(dst_key),
				))
