import database
import html_parser
import queue
import re
import threading
import utils


num_workers_per_domain = 5
write_db_queue = queue.Queue()


def _write_to_db(num_total_workers):
	with database.connect() as con:
		cur = con.cursor()
		remaining_workers = num_total_workers
		while remaining_workers > 0:
			item = write_db_queue.get()
			if item is None:
				remaining_workers -= 1
				print(f"Worker finishing {remaining_workers}/{num_total_workers}")
				continue
			domain_id, url, title, body, timestamp = item
			print(f"Inserting {url}")
			cur.execute("SELECT 1 FROM news WHERE domain_id = ? AND (url = ? OR title = ?)", (domain_id, url, title))
			if cur.fetchone() is None:
				cur.execute("SELECT ifnull(max(id)+1, 0) FROM news")
				news_id, = cur.fetchone()
				cur.execute("INSERT INTO news VALUES(?,?,?,?,?,?)", (news_id, domain_id, url, title, body, timestamp))
				con.commit()


def _bfs(domain_id, domain_url, domain_news_pattern, q, visited, depth):
	while q:
		item = q.get()
		if item is None:
			write_db_queue.put(None)
			break
		node_url, node_is_last = item
		if node_is_last:
			depth -= 1
			if depth == 0:
				for _ in range(num_workers_per_domain):
					q.put(None)
				continue
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
			new_last, _ = q.get(-1)
			q.put((new_last, True))

def main():
	with database.connect() as con:
		cur = con.cursor()
		cur.execute("SELECT id, url, news_pattern FROM domains")
		domains = cur.fetchall()
		threads = []
		num_domains = len(domains)
		num_total_workers = num_domains * num_workers_per_domain
		write_db_thread = threading.Thread(target=_write_to_db, args=(num_total_workers, ))
		write_db_thread.start()

		for domain_id, domain_url, domain_news_pattern_str in domains:
			domain_news_pattern = re.compile(rf"{domain_news_pattern_str}")
			domain_raw_html = html_parser.raw_html(domain_url)
			domain_graph = [href for href in html_parser.node_hrefs(domain_raw_html, domain_url)]
			num_domain_graph = len(domain_graph)

			q = queue.Queue()
			visited = set()
			visited.add(domain_url)
			depth = 1

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
