import json
import contextlib
import sqlite3


STATEMENTS = [
"""
CREATE TABLE IF NOT EXISTS domains(
	id  INTEGER NOT NULL PRIMARY KEY,
	url TEXT    NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS domain_news_pattern(
	domain_id INTEGER NOT NULL REFERENCES domains(id),
	pattern   TEXT    NOT NULL,
	PRIMARY KEY(domain_id, pattern)
);
""",
"""
CREATE TABLE IF NOT EXISTS domain_ignore_pattern(
	domain_id INTEGER NOT NULL REFERENCES domains(id),
	pattern   TEXT    NOT NULL,
	PRIMARY KEY(domain_id, pattern)
);
""",
"""
CREATE TABLE IF NOT EXISTS articles(
	id           INTEGER NOT NULL PRIMARY KEY,
	domain_id    INTEGER NOT NULL REFERENCES domains(id),
	url          TEXT    NOT NULL,
	title        TEXT,
	body         TEXT,
	publish_date TEXT    NOT NULL
);
"""
]


def connect(database="database2.sqlite", mode="rw"):
	return contextlib.closing(sqlite3.connect(f"file:{database}?mode={mode}", uri=True))


def _insert_domains(con, cur):
	with open("domains.json", "r") as f:
		domains = json.load(f)
		for item in domains:
			url = item["url"]
			news_pattern = item["news_pattern"]
			ignore_pattern = item["ignore_pattern"]
			cur.execute("SELECT 1 FROM domains WHERE url = ?", (url, ))
			if cur.fetchone() is None:
				cur.execute("SELECT ifnull(max(id)+1, 0) FROM domains")
				domain_id, = cur.fetchone()
				cur.execute("INSERT INTO domains VALUES(?,?)", (domain_id, url))

				for pattern in news_pattern:
					cur.execute("INSERT INTO domain_news_pattern VALUES(?,?)", (domain_id, pattern))

				for pattern in ignore_pattern:
					cur.execute("INSERT INTO domain_ignore_pattern VALUES(?,?)", (domain_id, pattern))
		con.commit()


def main():
	with connect(mode="rwc") as con:
		cur = con.cursor()
		for st in STATEMENTS:
			cur.execute(st)
		_insert_domains(con, cur)


if __name__ == "__main__":
	main()
