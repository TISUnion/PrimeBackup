import datetime


def timestamp_to_local_date(timestamp_ns: int, decimal: bool) -> str:
	local_time = datetime.datetime.fromtimestamp(timestamp_ns / 1e9)
	fmt = '%Y-%m-%d %H:%M:%S'
	if decimal:
		fmt += '.%f'
	return local_time.strftime(fmt)
