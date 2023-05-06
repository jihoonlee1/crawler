import database
import html_parser
import queue
import re
import threading
import utils


WRITE_DB_QUEUE = queue.Queue()
NUM_THREADS_PER_DOMAIN = 5


def _write_to_db(num_total_workers):
	with database.connect() as con:
		cur = con.cursor()
		current_workers = num_total_workers
		while current_workers > 0:
			item = WRITE_DB_QUEUE.get()
			if item is None:
				current_workers -= 1
				print(f"Worker finished {current_workers}/{num_total_workers}")
				continue
			domain_id, href, title, body, unix_timestamp = item
			cur.execute("SELECT ifnull(max(id)+1, 0) FROM news")
			news_id, = cur.fetchone()
			cur.execute("INSERT INTO news VALUES(?,?,?,?,?,?)", (news_id, domain_id, href, title, body, unix_timestamp))
			con.commit()


def _bfs(domain_id, domain_url, domain_news_pattern, scrap_queue, visited, depth):
	global WRITE_DB_QUEUE

	while scrap_queue:
		if depth == 0:
			WRITE_DB_QUEUE.put(None)
			break
		try:
			node_href, node_is_last = scrap_queue.get()
			print(f"Scraping: {node_href} Depth: {depth}")
			node_raw_html = html_parser.raw_html(node_href)
			if utils.is_news(domain_news_pattern, node_href):
				title, body, unix_timestamp = html_parser.news(node_raw_html)
				WRITE_DB_QUEUE.put((domain_id, node_href, title, body, unix_timestamp))

			node_graph = [href for href in html_parser.node_hrefs(node_raw_html, domain_url)]
			len_node_graph = len(node_graph)

			if node_is_last:
				depth -= 1

			for idx, item in enumerate(node_graph):
				if not item in visited:
					scrap_queue.put((item, False))
					visited.add(item)
				else:
					if (idx == len_node_graph - 1) and node_is_last:
						new_last, _ = scrap_queue.get(-1)
						scrap_queue.put((new_last, True))
		except Exception as e:
			print(e)
			continue


def main():
	with database.connect() as con:
		scrap_threads = []
		cur = con.cursor()
		cur.execute("SELECT id, url FROM domains")
		domains = cur.fetchall()
		num_domains = len(domains)
		num_total_workers = num_domains * NUM_THREADS_PER_DOMAIN

		write_db_thread = threading.Thread(target=_write_to_db, args=(num_total_workers, ))
		write_db_thread.start()

		for domain_id, domain_url in domains:
			cur.execute("SELECT pattern FROM domain_news_pattern WHERE domain_id = ?", (domain_id, ))
			domain_news_pattern_str, = cur.fetchone()
			domain_news_pattern = re.compile(rf"{domain_news_pattern_str}")

			depth = 1
			visited = set()
			scrap_queue = queue.Queue()
			visited.add(domain_url)

			domain_raw_html = html_parser.raw_html(domain_url)
			domain_graph = [href for href in html_parser.node_hrefs(domain_raw_html, domain_url)]
			len_domain_graph = len(domain_graph)

			for idx, href in enumerate(domain_graph):
				is_last = False
				if idx == len_domain_graph - 1:
					is_last = True
				visited.add(href)
				scrap_queue.put((href, is_last))

			for i in range(NUM_THREADS_PER_DOMAIN):
				t = threading.Thread(target=_bfs, args=(domain_id, domain_url, domain_news_pattern, scrap_queue, visited, depth))
				scrap_threads.append(t)
				t.start()

		for t in scrap_threads:
			t.join()

		write_db_thread.join()


if __name__ == "__main__":
	main()
