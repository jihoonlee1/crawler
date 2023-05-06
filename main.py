import requests
import bs4 
import urllib.parse
import queue
import threading
import goose3
import re
import database


SCRAP_QUEUE = queue.Queue()
WRITE_DB_QUEUE = queue.Queue()
NUM_SCRAP_THREADS = 2
VISITED = set()
LOCK = threading.Lock()
DEPTH = 1
NEWS_PATTERN = re.compile(r"https:\/\/www\.cnn\.com\/(2[0-9]{3})\/([0-9]{2})\/([0-9]{2})\/.+$")
ROOT = "https://www.cnn.com"


def _raw_html(href):
	r = requests.get(href)
	return r.text


def _html_doc(raw_html):
	html_doc = bs4.BeautifulSoup(raw_html, "html.parser")
	return html_doc


def _all_links_inc_dup(html_doc):
	links = html_doc.find_all("a", {"href": True})
	return links


def _relevant_links(links, root_netloc=ROOT):
	result = []
	for item in links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path
		target_netloc = temp.netloc
		if (target_netloc and target_netloc != root_netloc) or not target_path:
			continue
		result.append(item)
	return result


def _hrefs(relevant_links, root_netloc=ROOT):
	result = set()
	for item in relevant_links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path.strip()
		final_path = f"{root_netloc}{target_path}"
		result.add(final_path)
	return result


def _node_hrefs(node_raw_html):
	node_html_doc = _html_doc(node_raw_html)
	all_links_inc_dup = _all_links_inc_dup(node_html_doc)
	relevant_links = _relevant_links(all_links_inc_dup)
	return _hrefs(relevant_links)


def _article(raw_html):
	with goose3.Goose() as goose:
		article = goose.extract(raw_html=raw_html)
		return (article.title, article.cleaned_text, article.publish_date)


def _is_news(href):
	test = NEWS_PATTERN.search(href)
	if test is not None:
		return True
	return False


def _bfs():
	global SCRAP_QUEUE
	global DEPTH
	global WRITE_DB_QUEUE
	global LOCK

	while SCRAP_QUEUE:
		if DEPTH == 0:
			WRITE_DB_QUEUE.put(None)
			break
		node_href, node_is_last = SCRAP_QUEUE.get()
		print(f"Scraping: {node_href}")
		node_raw_html = _raw_html(node_href)
		if _is_news(node_href):
			title, body, publish_date = _article(node_raw_html)
			WRITE_DB_QUEUE.put((node_href, title, body, publish_date))
		node_graph = [href for href in _node_hrefs(node_raw_html)]

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
		print(f"Depth: {DEPTH}")


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
	root_raw_html = _raw_html(ROOT)
	root_graph = [href for href in _node_hrefs(root_raw_html)]
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
