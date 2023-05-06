import datetime
import re


pattern = re.compile(r"([1-9][0-9]{3})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})")


def time_to_unix(time_str):
	return int(datetime.datetime.strptime(find, "%Y-%m-%dT%H:%M:%S").timestamp())
