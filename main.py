import html_parser
import database
import threading
import queue
import re
import utils


num_workers_per_domain = 5
write_db_queue = queue.Queue()


def _write_to_db(num_total_workers):
	with database.connect() as con:
		cur = con.cursor()
		num_remaining_workers = num_total_workers
		while num_remaining_workers > 0:
			item = write_db_queue.get()
			if item is None:
				num_remaining_workers -= 1
			domain_id, url, title, body, timestamp = item
			cur.execute("SELECT ifnull(max(id)+1, 0) FROM news")
			news_id, = cur.fetchone()
			cur.execute("INSERT INTO news VALUES(?,?,?,?,?,?)", (news_id, domain_id, url, title, body, timestamp))
			con.commit()


def _bfs(domain_id, domain_url, domain_news_pattern, q, visited, depth):
	while q:
		item = q.get()
		if item is None:
			break
		node_url, node_is_last = item
		print(node_url)
		if node_is_last:
			depth -= 1
			if depth == 0:
				for _ in range(num_workers_per_domain):
					q.put(None)
					write_db_queue.put(None)
		try:
			node_raw_html = html_parser.raw_html(node_url)
			if utils.is_news(domain_news_pattern, node_url):
				title, body, timestamp = html_parser.news(node_raw_html)
				write_db_queue.put((domain_id, node_url, title, body, timestamp))
			node_graph = [href for href in html_parser.node_hrefs(node_raw_html, domain_url)]
			num_node_graph = len(node_graph)
			for idx, href in enumerate(node_graph):
				if href not in visited:
					visited.add(href)
					q.put((href, False))
		except:
			pass
		if node_is_last:
			new_last, _ = q.get()
			q.put(new_last, True)


def main():
	with database.connect() as con:
		cur = con.cursor()
		cur.execute("SELECT id, url FROM domains")
		domains = cur.fetchall()
		threads = []
		num_domains = len(domains)
		num_total_workers = num_domains * num_workers_per_domain
		write_db_thread = threading.Thread(target=_write_to_db, args=(num_total_workers, ))
		write_db_thread.start()

		for domain_id, domain_url in domains:
			cur.execute("SELECT pattern FROM domain_news_pattern WHERE domain_id = ?", (domain_id, ))
			domain_news_pattern_str, = cur.fetchone()
			domain_news_pattern = re.compile(rf"{domain_news_pattern_str}")
			domain_raw_html = html_parser.raw_html(domain_url)
			domain_graph = [href for href in html_parser.node_hrefs(domain_raw_html, domain_url)]
			num_domain_graph = len(domain_graph)

			q = queue.Queue()
			visited = set()
			visited.add(domain_url)
			depth = 2

			for idx, href in enumerate(domain_graph):
				is_last = False
				if idx == num_domain_graph-1:
					is_last = True
				q.put((href, is_last))
				visited.add(href)

			for _ in range(num_workers_per_domain):
				t = threading.Thread(target=_bfs, args=(domain_id, domain_url, domain_news_pattern, q, visited, depth))
				threads.append(t)
				t.start()

		for t in threads:
			t.join()

		write_db_thread.join()


if __name__ == "__main__":
	main()
