import sys, argparse, os, unidecode, traceback, datetime, time, codecs, re
import pandas as pd
from text_mining.tools import requests_helper
from scrapers.news_parser import NewsParsers, dummy_ticker
from url_processor import URLProcessor
from pyquery import PyQuery
from Utilities import find_tickers, get_global_timezone, utc, eastern
from articleDateExtractor import extractArticlePublishedDate
import dateparser
import settings, logging

try:
# For Python 3.0 and later
	from urllib.parse import unquote
except ImportError:
# Fall back to Python 2's urllib2
	from urllib2 import unquote

class TheStreet(NewsParsers):
	base_url = "https://www.thestreet.com/"

	def __init__(self, group='newswires', parse_articles = False, test_mode = False, timeout = 600, max_workers = 10, run_history = True, expire_redis_keys = False):
		super(TheStreet, self).__init__(domain='TheStreet', group=group, test_mode=test_mode, timeout=timeout, parse_articles=parse_articles, max_workers = max_workers, run_history = run_history,
			expire_redis_keys = expire_redis_keys)
		self.tickers = pd.read_csv(os.path.join(settings.data_directory,'tickers_export.csv'))['ticker']
		self.url_processor = URLProcessor(max_workers = max_workers)

	def parse_start_page(self, ticker):
		sys.stdout.write('\n Processing %s for %s\n'%(ticker, self.domain))
		start = 1
		page = 1
		while True:
			try:
				alerts = []
				source_url = TheStreet.base_url + 'quote/%s/details/news.html?start=%d&page=%d'%(ticker, start, page)
				sys.stdout.write('\n Processing %s for %s\n'%(source_url, self.domain))
				response = PyQuery(requests_helper(source_url).content)
				ticker_url_timestamp_map = {unquote(TheStreet.base_url + anchor.attrib['href'] if anchor.attrib['href'].startswith('/') else anchor.attrib['href']) : get_global_timezone(dateparser.parse(span_date.text))
							for anchor, span_date in zip(response('.news-list-compact__body a'), response('.news-list-compact__body time'))}
				if not len(ticker_url_timestamp_map):
					break

				news_urls = ticker_url_timestamp_map.keys()
				results = self.url_processor.process_urls(news_urls, timeout=self.timeout)
				if len(results):
					for url, extractor in results.items():
						if len(extractor.link_hash): #removes unavailable page
							link_hash = codecs.encode(url.encode('utf-8'), 'base64', 'strict')
							if link_hash not in self.cache:
								lud = get_global_timezone(datetime.datetime.utcnow(), local_tzinfo = utc)
								keywords = extractor.meta_keywords
								keywords.extend(extractor.keywords)
								tags = extractor.meta_data.get('tags', [])
								links = list(set(extractor.extractor.get_urls(extractor.html)))
								domain = extractor.source_url
								title = extractor.title
								timestamp = get_global_timezone(extractArticlePublishedDate(url, html=extractor.html))
								if timestamp is None:
									timestamp = ticker_url_timestamp_map.get(url)
								d = {		'url':url,
										'tags' : tags,
										'keywords': keywords,
										'title' : title,
										'links':links,
										'id' : link_hash,
										'source' : self.domain,
										'type' : self.group,
										'headline_domain' : domain,
										'tickers' : [ticker],
										'timestamp' : timestamp,
										'lud' : lud
									}
								self.process_article(d, extractor.html)
								tickers = []
								try:
									tickers = find_tickers(d['article'])
								except Exception as e:
									logging.error(e)
								d['tickers'].extend(tickers)
								d['tickers'] = list(set(d['tickers']))
								if len(d['tickers']) == 0:
									d['tickers'] = [dummy_ticker]
								alerts.append(d)
				if len(alerts):
					sys.stdout.write('\n Found %d new alerts for ticker %s'%(len(alerts), ticker))
				elif self.run_history == False:
					break
				if self.test_mode == False:
					self.insert_alerts(alerts)
				start = start + 30
				page = page + 1
			except Exception as e:
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
				logging.error(e, exc_info=1)

	def process_article(self, alert, html):
		regex = r"([\/][1][\/])"
		p = PyQuery(html)
		incr = 2
		max_page = 1
		alert['article'] = unidecode.unidecode(p('.article__body').text())
		pagination = p('#page-indicator').text()
		if len(pagination):
			max_page = int(p('#page-indicator').text().split('of')[-1].strip())
		while incr <= max_page:
			p = PyQuery(requests_helper(re.sub(regex, '/%d/'%(incr), alert['url'])).content)
			alert['article'] = alert['article'] + " " + unidecode.unidecode(p('.article__body').text())
			incr = incr + 1

	def runner(self, snooze=10.0):
		sys.stdout.write('\n' + self.domain + ' parser starting @ %s' % str(datetime.datetime.now()))
		self.update_cache()
		while(True):
			try:
				sys.stdout.write("\ncalling @ %s" % str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern)))
				for ticker in self.tickers:
					self.parse_start_page(ticker)
				if self.run_history:
					break
			except Exception as e:
				sys.stderr.write('\nController Error -> %s @ %s' % (e.message, datetime.datetime.now()))
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
				logging.error(e, exc_info=1)
			finally:
				sys.stdout.flush()
				sys.stderr.flush()
				time.sleep(snooze)

def main(args):
	run_history = args.run_history
	test_mode = args.test_mode
	snooze = args.snooze
	g = TheStreet(max_workers = 10, run_history=run_history, test_mode=test_mode)
	logging.basicConfig(filename=os.path.join(settings.log_directory,g.domain+'_error.log'),level=logging.ERROR, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z')
	g.runner(snooze = snooze)

if __name__== '__main__':
	try:
		__IPYTHON__
		sys.stdout.write('\nrunning via ipython -> not running continously')
	except NameError:
		argparser = argparse.ArgumentParser(description='The Street scraper')
		argparser.add_argument('--test_mode', action='store_true', default=False)
		argparser.add_argument('--run_history', action='store_true', default=False)
		argparser.add_argument('--snooze', default = 10.0, type=float)
		args = argparser.parse_args()
		main(args)
