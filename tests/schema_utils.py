from pathlib import Path
from typing import Dict

from pydantic import BaseModel
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine

# SQLite built-in objects (e.g. sqlite_sequence) are useless for testing
_SQLITE_INTERNAL_PREFIX = 'sqlite_'


class SchemaDDL(BaseModel):
	"""DDL strings for all user-defined tables and indexes in a SQLite database."""
	tables: Dict[str, str]  # table_name -> CREATE TABLE sql
	indexes: Dict[str, str]  # index_name -> CREATE INDEX sql


def collect_schema(engine: Engine) -> SchemaDDL:
	"""Query sqlite_master and return a :class:`SchemaDDL`.

	SQLite-internal objects (names starting with ``sqlite_``) are excluded.
	"""
	with engine.connect() as conn:
		def collect(sql: str) -> Dict[str, str]:
			return {
				row.name: row.sql.strip()
				for row in conn.execute(text(sql))
				if not row.name.startswith(_SQLITE_INTERNAL_PREFIX)
			}

		return SchemaDDL(
			tables=collect('SELECT name, sql FROM sqlite_master WHERE type="table" ORDER BY name'),
			indexes=collect('SELECT name, sql FROM sqlite_master WHERE type="index" AND sql IS NOT NULL ORDER BY name'),
		)


def schema_from_metadata(metadata: MetaData) -> SchemaDDL:
	engine = create_engine('sqlite://')
	metadata.create_all(engine)
	return collect_schema(engine)


def write_schema_yaml(schema: SchemaDDL, path: Path) -> None:
	data: Dict[str, Dict[str, LiteralScalarString]] = {
		'tables': {name: LiteralScalarString(sql) for name, sql in schema.tables.items()},
		'indexes': {name: LiteralScalarString(sql) for name, sql in schema.indexes.items()},
	}
	path.parent.mkdir(parents=True, exist_ok=True)
	yaml = YAML()
	yaml.default_flow_style = False
	yaml.width = 999  # prevent line-wrapping inside block scalars
	with open(path, 'w', encoding='utf-8') as f:
		yaml.dump(data, f)
