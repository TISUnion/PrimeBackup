from typing import TypeVar

from sqlalchemy.orm import Mapped

_T = TypeVar('_T')


def mapped_cast(obj: Mapped[_T]) -> _T:
	return obj
