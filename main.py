import database
import html_parser
import queue
import re
import threading
import utils


SCRAP_QUEUE = queue.Queue()
WRITE_DB_QUEUE = queue.Queue()
NUM_SCRAP_THREADS = 5
VISITED = set()
LOCK = threading.Lock()
DEPTH = 3


def _write_to_db():
	with database.connect() as con:
		cur = con.cursor()
		num_workers = NUM_SCRAP_THREADS
		while num_workers > 0:
			item = WRITE_DB_QUEUE.get()
			if item is None:
				num_workers -= 1
				continue
			domain_id, href, title, body, unix_timestamp = item
			cur.execute("SELECT ifnull(max(id)+1, 0) FROM news")
			news_id, = cur.fetchone()
			cur.execute("INSERT INTO news VALUES(?,?,?,?,?,?)", (news_id, domain_id, href, title, body, unix_timestamp))
			con.commit()


def _bfs(domain_id, root, news_pattern):
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
			if utils.is_news(news_pattern, node_href):
				title, body, unix_timestamp = html_parser.news(node_raw_html)
				WRITE_DB_QUEUE.put((domain_id, node_href, title, body, unix_timestamp))
			node_graph = [href for href in html_parser.node_hrefs(node_raw_html, root)]

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


def main():
	with database.connect() as con:
		cur = con.cursor()
		cnn_id = 0
		cur.execute("SELECT url FROM domains WHERE id = ?", (cnn_id, ))
		cnn_root, = cur.fetchone()
		cur.execute("SELECT pattern FROM domain_news_pattern WHERE domain_id = ?", (cnn_id, ))
		cnn_news_pattern_str, = cur.fetchone()

	cnn_news_pattern = re.compile(rf"{cnn_news_pattern_str}")
	VISITED.add(cnn_root)
	root_raw_html = html_parser.raw_html(cnn_root)
	root_graph = [href for href in html_parser.node_hrefs(root_raw_html, cnn_root)]
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
		t = threading.Thread(target=_bfs, args=(cnn_id, cnn_root, cnn_news_pattern))
		t.start()

	for t in scrap_threads:
		t.join()

	write_db_thread.join()


if __name__ == "__main__":
	main()
