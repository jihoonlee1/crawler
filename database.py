import contextlib
import sqlite3


STATEMENTS = [
"""
CREATE TABLE articles(
	id           INTEGER NOT NULL PRIMARY KEY,
	url          TEXT    NOT NULL,
	title        TEXT,
	body         TEXT,
	publish_date TEXT    NOT NULL
)
"""
]


def connect(database="database.sqlite", mode="rw"):
	return contextlib.closing(sqlite3.connect(f"file:{database}?mode={mode}", uri=True))


def main():
	with connect(mode="rwc") as con:
		cur = con.cursor()
		for st in STATEMENTS:
			cur.execute(st)


if __name__ == "__main__":
	main()
