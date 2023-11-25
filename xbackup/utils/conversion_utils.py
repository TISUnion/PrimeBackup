import datetime


def timestamp_to_local_date(timestamp_ns: int) -> str:
	timestamp_seconds = timestamp_ns / 1e9
	dt = datetime.datetime.fromtimestamp(timestamp_seconds)
	local_tz = datetime.datetime.now().astimezone().tzinfo
	return str(dt.astimezone(local_tz))
