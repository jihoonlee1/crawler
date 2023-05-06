import bs4
import goose3
import re
import requests
import urllib.parse
import utils


goose = goose3.Goose()


def _html_doc(raw_html):
	html_doc = bs4.BeautifulSoup(raw_html, "html.parser")
	return html_doc


def _all_links_inc_dup(html_doc):
	links = html_doc.find_all("a", {"href": True})
	return links


def _relevant_links(links, root_netloc):
	result = []
	for item in links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path
		target_netloc = temp.netloc
		if (target_netloc and target_netloc != root_netloc) or not target_path:
			continue
		result.append(item)
	return result


def _hrefs(relevant_links, root_netloc):
	result = set()
	for item in relevant_links:
		temp = urllib.parse.urlparse(item["href"])
		target_path = temp.path.strip()
		final_path = f"{root_netloc}{target_path}"
		result.add(final_path)
	return result


def node_hrefs(node_raw_html, root_netloc):
	node_html_doc = _html_doc(node_raw_html)
	all_links_inc_dup = _all_links_inc_dup(node_html_doc)
	relevant_links = _relevant_links(all_links_inc_dup, root_netloc)
	return _hrefs(relevant_links, root_netloc)


def news(raw_html):
	news = goose.extract(raw_html=raw_html)
	return (news.title, news.cleaned_text, utils.time_to_unix(news.publish_date))


def raw_html(href):
	r = requests.get(href)
	return r.text
