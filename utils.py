import datetime
import re


def time_to_unix(time_str):
	time_str = pattern.search(time_str).group(0)
	return int(datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S").timestamp())


def is_news(news_pattern, href):
	test = news_pattern.search(href)
	if test is not None:
		return True
	return False
