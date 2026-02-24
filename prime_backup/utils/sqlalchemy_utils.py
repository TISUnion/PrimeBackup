from typing import TypeVar, Union, cast

from sqlalchemy.orm import Mapped

_T = TypeVar('_T')


def mapped_cast(obj: Union[Mapped[_T], _T]) -> _T:
	return cast(_T, obj)
