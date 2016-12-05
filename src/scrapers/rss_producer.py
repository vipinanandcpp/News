import sys, argparse, os, time, kombu, settings
from url_processor import URLProcessor
from bson import json_util
import feedparser
from scrapers.news_parser import NewsParsers, redis_connection
from Utilities import rabbitmq_connection0
import rss_producer_rules
import logging

class RSS_PRODUCER(NewsParsers):
	def __init__(self, domain, group='rss',parse_articles = False, test_mode = False, timeout = 600, max_workers = 10, expire_redis_keys = False):
		super(RSS_PRODUCER, self).__init__(domain= domain, group=group, test_mode=test_mode, timeout=timeout, parse_articles=parse_articles, max_workers = max_workers, expire_redis_keys = expire_redis_keys)
		self.url_processor = URLProcessor(max_workers = max_workers)
		self.rss_producer_rule_instance = getattr(rss_producer_rules, self.domain)()
		if self.test_mode is False:
			self.start_producer()

	def __del__(self):
		rabbitmq_connection0.release()
		super(RSS_PRODUCER, self).__del__()

	def process_article(self, alert, article):
		return alert

	def parse_start_page(self, url):
		alerts = []
		data = RSS_PRODUCER.call_feedparser(url)
		if data.status != 304:
			articles = data.get('entries')
			alerts.extend(self.rss_producer_rule_instance.parse_feed(self, articles))
		return alerts

	def start_producer(self):
		sys.stdout.write('Starting producer for %s\n'%(self.domain))
		outgoing_exchange = kombu.Exchange(name=self.group, type='direct')
		self.rabbitmq_producer = kombu.Producer(rabbitmq_connection0, exchange=outgoing_exchange, serializer='json', compression=None, auto_declare=True)
		if rabbitmq_connection0.connected:
			outgoing_exchange(rabbitmq_connection0).declare()

	def runner(self, snooze=10):
		sys.stdout.write('\nProcessing RSS runner for %s\n'%(self.domain))
		while True:
			alerts = []
			for source_url in self.source_urls:
				_alerts = self.parse_start_page(source_url)
				alerts.extend(_alerts)
				if source_url in self.scheduled_source_urls:
					self.rabbitmq_producer.publish(json_util.dumps(_alerts), routing_key=self.group, serializer='json')
			if len(alerts):
				sys.stdout.write('\n Found %d new alerts for %s %s'%(len(alerts), self.domain, self.group))
				self.insert_alerts(alerts)
		time.sleep(snooze)

	@staticmethod
	def call_feedparser(url):
		modified = redis_connection.get(url + "_modified")
		etag = redis_connection.get(url + "_etag")
		d = feedparser.parse(url, modified=modified, etag=etag, request_headers={'Cache-control': 'max-age=0'})
		if d.status == 200:
		 	if hasattr(d, 'modified'):
		 		redis_connection.set(url + "_modified", d.modified)
		 	if hasattr(d, 'etag'):
		 		redis_connection.set(url + "_etag", d.etag)
		return d

def main(args):
	test_mode = args.test_mode
	snooze = args.snooze
	domain = args.domain
	metadata = {}
	with open('rss_feeds.json') as fp:
		metadata = json_util.loads(fp.read()).get(domain,{})
	if len(metadata):
		logging.basicConfig(filename=os.path.join(settings.log_directory,domain+'_error.log'),level=logging.ERROR, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z')
		rss_producer = RSS_PRODUCER(domain=domain, test_mode=test_mode)
		rss_producer.base_url = metadata['base_url']
		rss_producer.source_urls = metadata['source_urls']
		rss_producer.scheduled_source_urls = [source_url for source_url, is_scheduled in zip(metadata['source_urls'], metadata['scheduled']) if is_scheduled] if len(metadata.get('scheduled',[])) else []
		rss_producer.runner(snooze=snooze)

if __name__== '__main__':
	try:
		__IPYTHON__
		sys.stdout.write('\nrunning via ipython -> not running continously')
	except NameError:
		argparser = argparse.ArgumentParser(description='RSS producer')
		argparser.add_argument('--domain', action='store', dest='domain', help='domain', required=True)
		argparser.add_argument('--test_mode', action='store_true', default=False)
		argparser.add_argument('--snooze', default = 30.0, type=float)
		args = argparser.parse_args()
		main(args)






