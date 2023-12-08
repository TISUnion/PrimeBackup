from prime_backup.action import Action
from prime_backup.db.access import DbAccess
from prime_backup.types.db_meta_info import DbMetaInfo


class GetDbMetaAction(Action[DbMetaInfo]):
	def run(self) -> DbMetaInfo:
		with DbAccess.open_session() as session:
			return DbMetaInfo.of(session.get_db_meta())
