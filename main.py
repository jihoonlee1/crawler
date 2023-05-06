import database
import html_parser
import queue
import re
import threading


SCRAP_QUEUE = queue.Queue()
WRITE_DB_QUEUE = queue.Queue()
NUM_SCRAP_THREADS = 5
VISITED = set()
LOCK = threading.Lock()
DEPTH = 3
NEWS_PATTERN = re.compile(r"https:\/\/www\.cnn\.com\/(2[0-9]{3})\/([0-9]{2})\/([0-9]{2})\/.+$")
ROOT = "https://www.cnn.com"


def _bfs():
	global SCRAP_QUEUE
	global DEPTH
	global WRITE_DB_QUEUE
	global LOCK

	while SCRAP_QUEUE:
		if DEPTH == 0:
			WRITE_DB_QUEUE.put(None)
			break
		try:
			node_href, node_is_last = SCRAP_QUEUE.get()
			print(f"Scraping: {node_href} Depth: {DEPTH}")
			node_raw_html = html_parser.raw_html(node_href)
			if html.parser.is_news(NEWS_PATTERN, node_href):
				title, body, publish_date = html_parser.article(node_raw_html)
				WRITE_DB_QUEUE.put((node_href, title, body, publish_date))
			node_graph = [href for href in html_parser.node_hrefs(node_raw_html, ROOT)]

			len_node_graph = len(node_graph)
			if node_is_last:
				with LOCK:
					DEPTH -= 1

			for idx, item in enumerate(node_graph):
				if not item in VISITED:
					SCRAP_QUEUE.put((item, False))
					VISITED.add(item)
				else:
					if idx == len_node_graph-1 and node_is_last:
						new_last, _ = SCRAP_QUEUE.get(-1)
						SCRAP_QUEUE.put((new_last, True))
		except Exception as e:
			print(e)
			continue


def _write_to_db():
	with database.connect() as con:
		cur = con.cursor()
		num_workers = NUM_SCRAP_THREADS
		while num_workers > 0:
			item = WRITE_DB_QUEUE.get()
			if item is None:
				num_workers -= 1
				continue
			href, title, body, publish_date = item
			cur.execute("SELECT ifnull(max(id)+1, 0) FROM articles")
			article_id, = cur.fetchone()
			cur.execute("INSERT INTO articles VALUES(?,?,?,?,?)", (article_id, href, title, body, publish_date))
			con.commit()


def main():
	VISITED.add(ROOT)
	root_raw_html = html_parser.raw_html(ROOT)
	root_graph = [href for href in html_parser.node_hrefs(root_raw_html, ROOT)]
	len_root_graph = len(root_graph)
	scrap_threads = []
	write_db_thread = threading.Thread(target=_write_to_db)
	write_db_thread.start()

	for idx, item in enumerate(root_graph):
		is_last = False
		if idx == len_root_graph - 1:
			is_last = True
		if not item in VISITED:
			VISITED.add(item)
			SCRAP_QUEUE.put((item, is_last))

	for i in range(NUM_SCRAP_THREADS):
		t = threading.Thread(target=_bfs)
		t.start()

	for t in scrap_threads:
		t.join()

	write_db_thread.join()


if __name__ == "__main__":
	main()
