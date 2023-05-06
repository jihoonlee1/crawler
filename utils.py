import datetime
import re


pattern = re.compile(r"([1-9][0-9]{3})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})")


def time_to_unix(time_str):
	time_str = pattern.search(time_str).group(0)
	return int(datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S").timestamp())
