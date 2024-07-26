from typing import TypeVar, Union, Iterable

T = TypeVar('T')
OneOrMultiple = Union[Iterable[T], T]
