import sys
from pathlib import Path

__REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(__REPO_ROOT))

from tests import schema_utils

__DATA_DIR = __REPO_ROOT / 'tests' / 'data'


def main() -> None:
	from prime_backup.db.schema import Base as CurrentBase

	path = __DATA_DIR / 'schema_ddl_current.yml'
	ddl = schema_utils.schema_from_metadata(CurrentBase.metadata)
	schema_utils.write_schema_yaml(ddl, path)
	print(f'Written: {path}')


if __name__ == '__main__':
	main()
