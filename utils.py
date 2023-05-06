import datetime
import re


pattern0 = re.compile(r"([1-9][0-9]{3})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})")

months = {
	"Jan": "01",
	"Feb": "02",
	"Mar": "03",
	"Apr": "04",
	"May": "05",
	"Jun": "06",
	"Jul": "07",
	"Aug": "08",
	"Sep": "09",
	"Oct": "10",
	"Nov": "11",
	"Dec": "12"
}

tz_utc_diff_mapping = {
	"EDT": 4,
	"EST": 4
}


def time_to_unix(time_str):
	unix_time = None
	time_pattern_match = pattern0.search(time_str)
	if time_pattern_match is None:
		temp = re.sub(r".*Posted:", "", time_str, flags=re.IGNORECASE)
		temp = re.sub(r"\|.+$", "", temp).strip()
		mid = temp[3:-3].strip()
		month = months[temp[:3]]
		time_str = month + " " + mid
		tzdiff = tz_utc_diff_mapping[temp[-3:]]
		time = datetime.datetime.strptime(time_str, "%m %d, %Y %I:%M %p")
		time = time + datetime.timedelta(hours=tzdiff)
		unix_time = int(time.timestamp())
	else:
		time_str = time_pattern_match.group(0)
		unix_time = int(datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S").timestamp())
	return unix_time


def is_news(news_pattern, href):
	test = news_pattern.search(href)
	if test is not None:
		return True
	return False
