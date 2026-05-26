import datetime
import time
from typing import Union


def datetime_to_str(date: datetime.datetime, *, decimal: bool = False) -> str:
	fmt = '%Y-%m-%d %H:%M:%S'
	if decimal:
		fmt += '.%f'
	return date.strftime(fmt)


def timestamp_to_local_date_ns(timestamp_ns: int) -> datetime.datetime:
	return datetime.datetime.fromtimestamp(timestamp_ns / 1e9)


def timestamp_to_local_date_str_ns(timestamp_ns: int, *, decimal: bool = False) -> str:
	date = timestamp_to_local_date_ns(timestamp_ns)
	return datetime_to_str(date, decimal=decimal)


def seconds_to_timestamp_ns(timestamp_seconds: Union[int, float]) -> int:
	if isinstance(timestamp_seconds, int):
		return timestamp_seconds * (10 ** 9)
	if isinstance(timestamp_seconds, float):
		import decimal
		return int(decimal.Decimal(str(timestamp_seconds)) * (10 ** 9))
	raise TypeError(type(timestamp_seconds))


def date_to_timestamp_ns(s: str) -> int:
	formats = [
		'%Y',                    # 2023
		'%Y%m',                  # 202311
		'%Y%m%d',                # 20231130
		'%Y-%m-%d',              # 2023-11-30
		'%Y/%m/%d',              # 2023/11/30
		'%Y%m%d%H',              # 2023113021
		'%Y%m%d%H%M',            # 202311302139
		'%Y%m%d%H%M%S',          # 20231130213955
		'%Y%m%d %H%M%S',         # 20231130 213955
		'%Y%m%d %H:%M:%S',       # 20231130 21:39:55
		'%Y-%m-%d %H:%M:%S',     # 2023-11-30 21:39:55
		'%Y-%m-%d %H:%M:%S.%f',  # 2023-11-30 21:39:55.123
	]
	for fmt in formats:
		try:
			dt = datetime.datetime.strptime(s, fmt)
			timestamp_sec = int(time.mktime(dt.timetuple()))
			return timestamp_sec * (10 ** 9) + dt.microsecond * 1000
		except (ValueError, OSError):
			pass
	raise ValueError('cannot parse date from string {!r}'.format(s))
