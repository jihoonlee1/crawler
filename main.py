import requests
import bs4 
import urllib.parse
import queue
import threading
import goose3
import re


SCRAP_QUEUE = queue.Queue()
NUM_SCRAP_THREADS = 3
VISITED = set()
LOCK = threading.Lock()
DEPTH = 1
NEWS_PATTERN = re.compile(r"https:\/\/www\.cnn\.com\/(2[0-9]{3})\/([0-9]{2})\/([0-9]{2})\/.+$")


def _get_raw_html(root):
	r = requests.get(root)
	return r.text


def _get_article(raw_html):
	with goose3.Goose() as goose:
		return goose.extract(raw_html=raw_html)


def _get_html_doc(raw_html):
	html_doc = bs4.BeautifulSoup(raw_html, "html.parser")
	return html_doc


def _get_all_links(html_doc):
	links = html_doc.find_all("a", {"href": True})
	return links


def is_news(href):
	return NEWS_PATTERN.search(href)


def _get_relevant_links(links, root_netloc):
	result = []
	for item in links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path
		target_netloc = temp.netloc
		if (target_netloc and target_netloc != root_netloc) or not target_path:
			continue
		result.append(item)
	return result


def _get_final_hrefs(relevant_links, root_netloc):
	result = set()
	for item in relevant_links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path.strip()
		final_path = f"https://{root_netloc}{target_path}"
		result.add(final_path)
	return result


def _article(raw_html):
	with goose3.Goose() as goose:
		article = goose.extract(raw_html=raw_html)
		return (article.title, article.body, article.publish_date)


def _hrefs(root):
	root_netloc = urllib.parse.urlparse(root).netloc
	raw_html = _get_raw_html(root)
	html_doc = _get_html_doc(raw_html)
	links = _get_all_links(html_doc)
	relevant_links = _get_relevant_links(links, root_netloc)
	final_hrefs = _get_final_hrefs(relevant_links, root_netloc)
	return final_hrefs


def _bfs():
	global SCRAP_QUEUE
	global VISITED
	global DEPTH

	while SCRAP_QUEUE:
		if DEPTH == 0:
			break
		root, root_is_last = SCRAP_QUEUE.get()
		print(root, root_is_last)
		graph = [href for href in _hrefs(root)]
		len_graph = len(graph)
		if root_is_last:
			with LOCK:
				DEPTH -= 1

		for idx, item in enumerate(graph):
			if not item in VISITED:
				SCRAP_QUEUE.put((item, False))
				VISITED.add(item)
			else:
				if idx == len_graph-1 and root_is_last:
					new_last, _ = SCRAP_QUEUE.get(-1)
					SCRAP_QUEUE.put((new_last, True))
		print(f"Depth: {DEPTH}")


def main():
	root = "https://www.cnn.com/"
	VISITED.add(root)
	graph = [href for href in _hrefs(root)]
	scrap_threads = []
	len_graph = len(graph)

	for idx, item in enumerate(graph):
		is_last = False
		if idx == len_graph - 1:
			is_last = True
		if not item in VISITED:
			VISITED.add(item)
			SCRAP_QUEUE.put((item, is_last))

	for i in range(NUM_SCRAP_THREADS):
		t = threading.Thread(target=_bfs)
		t.start()

	for t in scrap_threads:
		t.join()


if __name__ == "__main__":
	main()
