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
		# Migration ends

		return json.dumps(config) != prev_state

	def __1_rename_pre_restore_backup_to_temporary(self, config: dict):
		"""
		Change in v1.7.0
		"""
		prune_config = config.get('prune', {})

		src_key = 'pre_restore_backup'
		dst_key = 'temporary_backup'
		if src_key in prune_config and dst_key not in prune_config:
			prune_config[dst_key] = prune_config.pop(src_key)
			self.logger.info('Renamed prune config key {!r} -> {!r}'.format(src_key, dst_key))
