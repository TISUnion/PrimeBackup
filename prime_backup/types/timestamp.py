import datetime
import time


class Timestamp:
	def __init__(self, timestamp_ns: int):
		self.__timestamp_ns = timestamp_ns

	@property
	def unix_sec(self) -> float:
		return self.__timestamp_ns / 1e9

	@property
	def unix_ms(self) -> float:
		return self.__timestamp_ns / 1e6

	@property
	def unix_us(self) -> float:
		return self.__timestamp_ns / 1e3

	@property
	def unix_ns(self) -> int:
		return self.__timestamp_ns

	@property
	def sec_part(self) -> int:
		return self.__timestamp_ns // (10 ** 9)

	@property
	def ns_part(self) -> int:
		return self.__timestamp_ns % (10 ** 9)

	def to_local_date(self) -> datetime.datetime:
		return datetime.datetime.fromtimestamp(self.unix_sec)

	@classmethod
	def now(cls) -> 'Timestamp':
		return cls(time.time_ns())

	@classmethod
	def from_second(cls, timestamp_seconds: float):
		return cls(int(timestamp_seconds * (10 ** 9)))

	@classmethod
	def from_second_and_nano(cls, timestamp_seconds: int, nanosecond_part: int):
		return cls(timestamp_seconds * (10 ** 9) + nanosecond_part)

	def __repr__(self):
		return f'Timestamp(ns={self.__timestamp_ns})'

	def __eq__(self, other) -> bool:
		return type(self) is type(other) and self.__timestamp_ns == other.__timestamp_ns

	def __lt__(self, other: 'Timestamp') -> bool:
		return self.__timestamp_ns < other.__timestamp_ns

	def __hash__(self) -> int:
		return hash(self.__timestamp_ns)
