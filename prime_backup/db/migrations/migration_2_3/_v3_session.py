from pathlib import Path
from typing import Optional, Sequence, Dict, Any, cast
from typing import TypeVar, List

from sqlalchemy import text, CursorResult
from sqlalchemy.orm import Session

from prime_backup.utils import collection_utils

_T = TypeVar('_T')


# make type checker happy
def _list_it(seq: Sequence[_T]) -> List[_T]:
	if not isinstance(seq, list):
		seq = list(seq)
	return seq


def _int_or_0(value: Optional[int]) -> int:
	if value is None:
		return 0
	return int(value)


class _V3DbSession:
	def __init__(self, session: Session, db_path: Path = None):
		self.session = session
		self.db_path = db_path
		self.__safe_var_limit = 999 - 20

	def v3_insert(self, table: str, data: Dict[str, Any], *, need_result: bool = False, id_key: str = 'id') -> Dict[str, Any]:
		if table not in ['file', 'fileset', 'backup']:
			raise ValueError(f'unknown table name {table!r}')

		columns = list(data.keys())

		columns_str = ', '.join(columns)
		placeholders_str = ', '.join(f':{col}' for col in columns)

		stmt = text(f'INSERT INTO {table} ({columns_str}) VALUES ({placeholders_str})').bindparams(**data)
		insert_result = cast(CursorResult, self.session.execute(stmt))

		if need_result:
			query_stmt = text(f'SELECT * FROM {table} WHERE {id_key} = :id').bindparams(id=insert_result.lastrowid)
			result_row = self.session.execute(query_stmt).fetchone()
			# noinspection PyProtectedMember
			return dict(result_row._mapping)
		else:
			return data

	def v3_get_fileset_associated_backup_count(self, fileset_id: int) -> int:
		return _int_or_0(self.session.execute(
			text('SELECT count(*) FROM backup WHERE fileset_id_base = :fileset_id OR fileset_id_delta = :fileset_id').
			bindparams(fileset_id=fileset_id)
		).scalar_one())

	def v3_get_fileset_delta_file_object_count_sum(self, base_fileset_id: int) -> int:
		"""For those backups whose base fileset is the given one, sum up the file_object_count of their delta filesets"""
		delta_fileset_ids = _list_it(self.session.execute(
			text('SELECT distinct fileset_id_delta FROM backup WHERE fileset_id_base = :base_fileset_id').
			bindparams(base_fileset_id=base_fileset_id)
		).scalars().all())

		count_sum = 0
		for view in collection_utils.slicing_iterate(_list_it(delta_fileset_ids), self.__safe_var_limit):
			sql_in_arg = '({})'.format(','.join(map(str, view)))
			count_sum += _int_or_0(self.session.execute(
				text(f'SELECT sum(file_object_count) FROM fileset WHERE id IN {sql_in_arg}')
			).scalar_one())
		return count_sum

	def v3_get_last_n_base_fileset(self, limit: int) -> List[dict]:
		fileset_rows = self.session.execute(text('SELECT * FROM fileset WHERE base_id = 0 ORDER BY id DESC LIMIT :limit').bindparams(limit=limit))
		# noinspection PyProtectedMember
		return [dict(fileset_row._mapping) for fileset_row in fileset_rows]
	
	def v3_get_fileset_files(self, fileset_id: int) -> List[dict]:
		file_rows = self.session.execute(text('SELECT * FROM file WHERE fileset_id = :fileset_id').bindparams(fileset_id=fileset_id))
		# noinspection PyProtectedMember
		return [dict(file_row._mapping)	for file_row in file_rows]
