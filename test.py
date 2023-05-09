import html_parser


root = "https://www.cbsnews.com/"
raw = html_parser.raw_html(root)
links = html_parser.node_hrefs(raw, root)
for item in links:
	print(item)
