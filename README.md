# Web Crawler

## Introduction
I wrote a simple, well functioning web crawler from scratch, using threads, bfs, and sqlite database to store the news.
I've tested CNN with depth of 3, 5 thread workers for scraping and it seems to work well.

Each domain have pattern of url that are recognized as news, and that is what I store in the database.
Each news will have the domain (where the news is from), title, body, and unix_timestamp to which the news was published.
