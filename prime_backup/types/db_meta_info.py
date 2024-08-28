import dataclasses

from prime_backup.db import schema


@dataclasses.dataclass(frozen=True)
class DbMetaInfo:
	magic: int
	version: int
	hash_method: str

	@classmethod
	def of(cls, meta: schema.DbMeta) -> 'DbMetaInfo':
		"""
		Notes: should be inside a session
		"""
		return DbMetaInfo(
			magic=meta.magic,
			version=meta.version,
			hash_method=meta.hash_method,
		)
