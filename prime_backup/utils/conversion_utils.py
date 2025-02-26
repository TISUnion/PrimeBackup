import datetime


def datetime_to_str(date: datetime.datetime, *, decimal: bool = False) -> str:
	fmt = '%Y-%m-%d %H:%M:%S'
	if decimal:
		fmt += '.%f'
	return date.strftime(fmt)


def timestamp_to_local_date_us(timestamp_us: int) -> datetime.datetime:
	return datetime.datetime.fromtimestamp(timestamp_us / 1e6)


def timestamp_to_local_date_str_us(timestamp_us: int, *, decimal: bool = False) -> str:
	date = timestamp_to_local_date_us(timestamp_us)
	return datetime_to_str(date, decimal=decimal)


def date_to_timestamp_us(s: str) -> int:
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
			return int(dt.timestamp() * 1e6)
		except ValueError:
			pass
	raise ValueError('cannot parse date from string {!r}'.format(s))
