import json
import contextlib
import pathlib
import sqlite3


dbpath = "database.sqlite"
statements = [
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
CREATE TABLE IF NOT EXISTS news(
	id             INTEGER NOT NULL PRIMARY KEY,
	domain_id      INTEGER NOT NULL REFERENCES domains(id),
	url            TEXT    NOT NULL,
	title          TEXT,
	body           TEXT,
	unix_timestamp INTEGER NOT NULL
);
"""
]


def connect(database=dbpath, mode="rw"):
	return contextlib.closing(sqlite3.connect(f"file:{database}?mode={mode}", uri=True))


def _insert_domains():
	with connect() as con:
		cur = con.cursor()
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
						cur.execute("INSERT OR IGNORE INTO domain_news_pattern VALUES(?,?)", (domain_id, pattern))

					for pattern in ignore_pattern:
						cur.execute("INSERT OR IGNORE INTO domain_ignore_pattern VALUES(?,?)", (domain_id, pattern))
			con.commit()


def _initialize():
	if not pathlib.Path(dbpath).exists():
		with connect(mode="rwc") as con:
			cur = con.cursor()
			for st in statements:
				cur.execute(st)


def main():
	_initialize()
	_insert_domains()


if __name__ == "__main__":
	main()
