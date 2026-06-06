from typing import TYPE_CHECKING, Optional, TypeVar

if TYPE_CHECKING:
	from prime_backup.types.db_meta_info import DbMetaInfo
	from prime_backup.types.hash_method import HashMethod, HasherCreator, HashableBuffer, Hasher

_T = TypeVar('_T')


class DbMetaCache:
	__meta: Optional['DbMetaInfo'] = None
	__hash_method: Optional['HashMethod'] = None
	__create_hasher_func: Optional['HasherCreator'] = None

	@classmethod
	def set(cls, meta: Optional['DbMetaInfo']):
		if meta is None:
			cls.reset()
			return

		cls.__meta = meta
		from prime_backup.types.hash_method import HashMethod
		try:
			hash_method = HashMethod[meta.hash_method]
		except KeyError:
			raise ValueError('invalid hash method {!r} in db meta'.format(meta.hash_method)) from None
		cls.__hash_method = hash_method
		cls.__create_hasher_func = hash_method.value.create_hasher

	@classmethod
	def get(cls) -> Optional['DbMetaInfo']:
		return cls.__meta

	@classmethod
	def reset(cls):
		cls.__meta = None
		cls.__hash_method = None

	@classmethod
	def get_hash_method(cls) -> 'HashMethod':
		return cls.__ensure_not_none(cls.__hash_method)

	@classmethod
	def _get_hash_method_no_check(cls) -> 'HashMethod':
		# faster than get_hash_method(), useful for hot paths
		assert cls.__hash_method is not None
		return cls.__hash_method

	@classmethod
	def _create_hasher_no_check(cls, buf: 'HashableBuffer' = b'') -> 'Hasher':
		assert cls.__create_hasher_func is not None
		return cls.__create_hasher_func(buf)

	@classmethod
	def __ensure_not_none(cls, value: Optional[_T]) -> _T:
		if value is None:
			raise RuntimeError('db meta not is not initialized yet')
		return value
