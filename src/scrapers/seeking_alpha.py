import sys, argparse, os, unidecode, traceback, datetime, time, requests, codecs
from bson import json_util
import pandas as pd
from text_mining.tools import requests_helper
from scrapers.news_parser import NewsParsers, dummy_ticker
from url_processor import URLProcessor
from pyquery import PyQuery
from Utilities import find_tickers, get_global_timezone, eastern, utc
from articleDateExtractor import extractArticlePublishedDate
import dateparser
import settings, logging

try:
# For Python 3.0 and later
	from urllib.parse import unquote
except ImportError:
# Fall back to Python 2's urllib2
	from urllib2 import unquote

class SeekingAlpha(NewsParsers):
	base_url = "http://seekingalpha.com/"

	def create_source_urls_map(self):
		d = {}
		for ticker in self.tickers:
			#d.setdefault(ticker, {}).setdefault('latest','symbol/%s'%(ticker))
			#d.setdefault(ticker, {}).setdefault('focus','symbol/%s/focus'%(ticker))
			#d.setdefault(ticker, {}).setdefault('related','symbol/%s/related'%(ticker))
			d.setdefault(ticker, {}).setdefault('news-all','symbol/%s/news'%(ticker))
			d.setdefault(ticker, {}).setdefault('press_release','tags/news/prs-data?slug=%s&page='%(ticker))
			d.setdefault(ticker, {}).setdefault('other_news','tags/news/other-news?slug=%s&page='%(ticker))
		return d


	def __init__(self, group='newswires', parse_articles = False, test_mode = False, timeout = 600, max_workers = 10, run_history = True, expire_redis_keys = False):
		super(SeekingAlpha, self).__init__(domain='SeekingAlpha', group=group, test_mode=test_mode, timeout=timeout, parse_articles=parse_articles, max_workers = max_workers, run_history = run_history,
			expire_redis_keys = expire_redis_keys)
		self.tickers = pd.read_csv(os.path.join(settings.data_directory,'tickers_export.csv'))['ticker']
		self.source_urls = self.create_source_urls_map()
		self.url_processor = URLProcessor(max_workers = max_workers)
		self.loop_exit = False

	def parse_start_page(self, ticker):
		try:
			sys.stdout.write('\n Processing %s for %s\n'%(ticker, self.domain))
			source_urls_map = self.source_urls.get(ticker)
			for source, source_url in source_urls_map.items():
				self.loop_exit = False
				sys.stdout.write('\n Running extraction for %s'%(source))
				source_url = SeekingAlpha.base_url + source_url
				if source in ('news-all'):
					r = requests.get(source_url)
					response = PyQuery(r.content)
					page_links = [t.text() for t in response('#paging a').items()]
					max_page = 1
					if len(page_links):
						max_page = int(page_links[-2])
					for page in range(1,max_page+1):
						sys.stdout.write('\n Processing page %d for %s\n'%(page, source))
						alerts = self._process_source_links(source, source_url+'/%d'%(page), ticker)
						if len(alerts):
							sys.stdout.write('\n Found %d new alerts for ticker %s'%(len(alerts), ticker))
						if self.test_mode == False:
							self.insert_alerts(alerts)

				elif source in ('press_release', 'other_news'):
					page = 1
					while True:
						sys.stdout.write('\n Processing page %d for %s\n'%(page, source))
						alerts = self._process_source_links(source, source_url+'%d'%(page), ticker)
						page = page + 1
						if len(alerts):
							sys.stdout.write('\n Found %d new alerts for ticker %s'%(len(alerts), ticker))
						if self.test_mode == False:
							self.insert_alerts(alerts)
						if self.loop_exit:
							break
						elif not len(alerts) and self.run_history == False:
							break
		except Exception as e:
			sys.stderr.write('\n\t' + str(traceback.print_exc()))
			logging.error(e, exc_info=1)

	def _process_source_links(self, source, source_url, ticker):
		results = {}
		ticker_url_timestamp_map = {}
		alerts = []
		r = requests_helper(source_url)
		response = PyQuery(r.content)
		if source in ('news-all'):
			ticker_url_timestamp_map = {unquote(self.base_url+anchor.attrib['href']) : get_global_timezone(dateparser.parse(span_date.text))
							for anchor, span_date in zip(response('.market_current_title'), response('.date.pad_on_summaries'))}
			news_urls = ticker_url_timestamp_map.keys()
			results = self.url_processor.process_urls(news_urls, timeout=self.timeout)
		elif source in ('press_release', 'other_news'):
			json_data = json_util.loads(response.text())
			ticker_url_timestamp_map = {unquote(data['uri']) if source == 'other_news' else unquote(self.base_url+data['uri']): get_global_timezone(dateparser.parse(data['publish_on'])) for data in json_data}
			news_urls = ticker_url_timestamp_map.keys()
			results = self.url_processor.process_urls(news_urls, timeout=self.timeout)
			if not len(results):
				self.loop_exit = True
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
						text = extractor.text
						timestamp = get_global_timezone(extractArticlePublishedDate(url, html=extractor.html))
						if timestamp is None:
							timestamp = ticker_url_timestamp_map.get(url)
						d = {		'url':url,
								'article':unidecode.unidecode(text),
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
		return alerts

	def process_article(self, alert, article):
		return alert

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
				sys.stderr.write('\nController Error -> %s @ %s' % (str(e), datetime.datetime.now()))
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
	g = SeekingAlpha(max_workers = 10, run_history=run_history, test_mode=test_mode)
	logging.basicConfig(filename=os.path.join(settings.log_directory,g.domain+'_error.log'),level=logging.ERROR, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z')
	g.runner(snooze = snooze)

if __name__== '__main__':
	try:
		__IPYTHON__
		sys.stdout.write('\nrunning via ipython -> not running continously')
	except NameError:
		argparser = argparse.ArgumentParser(description='Seeking Alpha scraper')
		argparser.add_argument('--test_mode', action='store_true', default=False)
		argparser.add_argument('--run_history', action='store_true', default=False)
		argparser.add_argument('--snooze', default = 10.0, type=float)
		args = argparser.parse_args()
		main(args)
