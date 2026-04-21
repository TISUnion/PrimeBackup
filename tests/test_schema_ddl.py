"""
How to regenerate the frozen_answer values -- Run the helper script from the repo root:

    python tests/tools/dump_schema_ddl.py

Review the diff of the updated YAML files and commit them as the new golden values
"""
import re
import unittest
from pathlib import Path

from ruamel.yaml import YAML

from prime_backup.utils import db_utils
from tests import schema_utils

__DATA_DIR = Path(__file__).parent / 'data'


def __load_expected(filename: str) -> schema_utils.SchemaDDL:
	with open(__DATA_DIR / filename, encoding='utf-8') as f:
		obj = YAML(typ='safe').load(f)
		return schema_utils.SchemaDDL.model_validate(obj)


def _assert_schema_matches(tc: unittest.TestCase, actual: schema_utils.SchemaDDL, filename: str, exact_match: bool):
	if not db_utils.check_sqlite_without_rowid():
		tc.fail(f'SQLite without rowid is not supported, version: {db_utils.get_sqlite_version()}')

	frozen_answer = __load_expected(filename)
	hint = f'Re-run tests/tools/dump_schema_ddl.py to update {filename}.'
	if exact_match:
		tc.assertEqual(actual.tables, frozen_answer.tables, f'table DDL mismatch – {hint}')
		tc.assertEqual(actual.indexes, frozen_answer.indexes, f'index DDL mismatch – {hint}')
	else:
		# actual may be a subset of frozen_answer; every table present in actual must match frozen_answer
		for table_name, actual_sql in actual.tables.items():
			tc.assertIn(table_name, frozen_answer.tables, f'unexpected table {table_name!r} not found in frozen_answer schema – {hint}')
			tc.assertEqual(actual_sql, frozen_answer.tables[table_name], f'DDL mismatch for table {table_name!r} – {hint}')

		# only check indexes whose table is present in actual
		def _index_table(sql: str) -> str:
			"""Extract the table name from a CREATE INDEX … ON <table> (…) statement."""
			m = re.search(r'\bON\s+"?(\w+)"?\s*\(', sql, re.IGNORECASE)
			return m.group(1) if m else ''

		for index_name, actual_sql in actual.indexes.items():
			tc.assertIn(index_name, frozen_answer.indexes, f'unexpected index {index_name!r} not found in frozen_answer schema – {hint}')
			tc.assertEqual(actual_sql, frozen_answer.indexes[index_name], f'DDL mismatch for index {index_name!r} – {hint}')

		# every frozen_answer index whose table IS covered by actual must also be present in actual
		for index_name, expected_sql in frozen_answer.indexes.items():
			table = _index_table(expected_sql)
			if table in actual.tables:
				tc.assertIn(index_name, actual.indexes, f'index {index_name!r} (on table {table!r}) is missing from actual – {hint}')


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestV3SchemaDDL(unittest.TestCase):
	def test_tables_and_indexes(self):
		from prime_backup.db.migrations.migration_2_3.migration_2_3 import _V3
		_assert_schema_matches(self, schema_utils.schema_from_metadata(_V3.Base.metadata), 'schema_ddl_v3.yml', exact_match=False)


class TestV4SchemaDDL(unittest.TestCase):
	def test_tables_and_indexes(self):
		from prime_backup.db.migrations.migration_3_4 import _V4
		_assert_schema_matches(self, schema_utils.schema_from_metadata(_V4.Base.metadata), 'schema_ddl_v4.yml', exact_match=False)


class TestCurrentSchemaDDL(unittest.TestCase):
	def test_tables_and_indexes(self):
		from prime_backup.db.schema import Base as CurrentBase
		_assert_schema_matches(self, schema_utils.schema_from_metadata(CurrentBase.metadata), 'schema_ddl_v4.yml', exact_match=True)


if __name__ == '__main__':
	unittest.main()
