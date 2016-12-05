import sys
from time import mktime
import unidecode, traceback, datetime, codecs
from scrapers.news_parser import dummy_ticker
from pyquery import PyQuery
from Utilities import find_tickers, get_global_timezone, utc, eastern, gmt, central_US_timezone
from articleDateExtractor import extractArticlePublishedDate
import logging

try:
# For Python 3.0 and later
	from urllib.parse import unquote
except ImportError:
# Fall back to Python 2's urllib2
	from urllib2 import unquote


def parse_feed(rss_producer_instance, articles, local_tzinfo = eastern):
	results = rss_producer_instance.url_processor.process_urls([article['link'] for article in articles], timeout=rss_producer_instance.timeout)
	article_url_map = {article['link'] : article for article in articles}
	alerts = []
	if len(results):
		for url, extractor in results.items():
			if len(extractor.link_hash): #removes unavailable page
				d={}
				link_hash = codecs.encode(url.encode('utf-8'), 'base64', 'strict')
				if link_hash not in rss_producer_instance.cache:
					lud = get_global_timezone(datetime.datetime.utcnow(), local_tzinfo = utc)
					keywords = extractor.meta_keywords
					keywords.extend(extractor.keywords)
					tags = extractor.meta_data.get('tags', [])
					links = list(set(extractor.extractor.get_urls(extractor.html)))
					domain = extractor.source_url
					title = article_url_map[url]['title']
					text = extractor.text
					publish_timestamp = get_global_timezone(article_url_map[url].get('published'), local_tzinfo=local_tzinfo)
					if publish_timestamp is None:
						if article_url_map[url].get('published_parsed') is not None:
							publish_timestamp = get_global_timezone(datetime.datetime.fromtimestamp(mktime(article_url_map[url].get('published_parsed'))), local_tzinfo = utc)
						elif article_url_map[url].get('updated_parsed') is not None:
							publish_timestamp = get_global_timezone(datetime.datetime.fromtimestamp(mktime(article_url_map[url].get('updated_parsed'))), local_tzinfo = utc)
					if publish_timestamp is None:
						publish_timestamp = get_global_timezone(extractArticlePublishedDate(url, html=extractor.html))
					d = {	'url':url,
						'article':unidecode.unidecode(text),
						'tags' : tags,
						'keywords': keywords,
						'title' : title,
						'links':links,
						'id' : link_hash,
						'source' : rss_producer_instance.domain,
						'type' : rss_producer_instance.group,
						'headline_domain' : domain,
						'timestamp' : publish_timestamp,
						'lud' : lud
						}

					try:
						d['tickers'] = find_tickers(d['article'])
						d['tickers'] = list(set(d['tickers']))
					except Exception as e:
						sys.stderr.write('\n\t' + str(traceback.print_exc()))
						logging.error(e)
					if len(d['tickers']) == 0:
						d['tickers'] = [dummy_ticker]
					alerts.append(d)
	return alerts


class CNN(object):
	def parse_feed(self, rss_producer_instance, articles):
		alerts = parse_feed(rss_producer_instance, articles, local_tzinfo=eastern)
		return alerts

class ELUNIVERSAL(object):
	def parse_feed(self, rss_producer_instance, articles):
		alerts = parse_feed(rss_producer_instance, articles, local_tzinfo=eastern)
		return alerts

class JORNADA(object):
	def parse_feed(self, rss_producer_instance, articles):
		alerts = parse_feed(rss_producer_instance, articles, local_tzinfo=gmt)
		return alerts

class BusinessInsider(object):
	def parse_feed(self, rss_producer_instance, articles):
		alerts = parse_feed(rss_producer_instance, articles, local_tzinfo=eastern)
		return alerts



