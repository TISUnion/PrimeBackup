import enum


class NotificationEvent(enum.Enum):
	backup_start = 'backup_start'
	backup_success = 'backup_success'
	backup_failure = 'backup_failure'
	restore_start = 'restore_start'
	restore_success = 'restore_success'
	restore_failure = 'restore_failure'

	@property
	def task(self) -> str:
		return self.value.split('_', 1)[0]

	@property
	def status(self) -> str:
		return self.value.split('_', 1)[1]
