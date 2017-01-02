import sys, argparse, os, time, settings, threading
from pika_publisher import PIKA_PUBLISHER
from url_processor import URLProcessor
from bson import json_util
import feedparser
from scrapers.news_parser import NewsParsers, redis_connection
from Utilities import hostname0
import rss_producer_rules
import logging


class RSS_PRODUCER(NewsParsers):
	def __init__(self, domain, group='rss',parse_articles = False, test_mode = False, timeout = 600, max_workers = 10, expire_redis_keys = False):
		super(RSS_PRODUCER, self).__init__(domain= domain, group=group, test_mode=test_mode, timeout=timeout, parse_articles=parse_articles, max_workers = max_workers, expire_redis_keys = expire_redis_keys)
		self.url_processor = URLProcessor(max_workers = max_workers)
		self.rss_producer_rule_instance = getattr(rss_producer_rules, self.domain)()
		self.pika_publisher = None
		if self.test_mode is False:
			try:
				self.pika_publisher = PIKA_PUBLISHER(amqp_url = "amqp://%s"%(hostname0), exchange_name = self.group, routing_key = self.group, queue_name = self.group, exchange_type = 'direct')
				self.start_producer()
			except Exception as e:
				sys.stderr.write('Error connecting to RabbitMQ\n -> %s'%(e))

	def __del__(self):
		super(RSS_PRODUCER, self).__del__()
		if self.pika_publisher is not None:
			sys.stdout.write("Shutting down RSS producer channel\n")
			self.pika_publisher.stop()

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
		threading.Thread(target = self.pika_publisher.run).start()

	def runner(self, snooze=10):
		sys.stdout.write('\nProcessing RSS runner for %s\n'%(self.domain))
		while True:
			alerts = []
			for source_url in self.source_urls:
				__alerts = []
				for alert in self.parse_start_page(source_url):
					if alert['id'] not in self.cache:
						alerts.append(alert)
						__alerts.append(alert)
				if self.test_mode is False:
					if self.pika_publisher is not None:
						if len(__alerts):
							if source_url in self.scheduled_source_urls:
								self.pika_publisher.publish_messages(__alerts)
				del __alerts
			if len(alerts):
				sys.stdout.write('\n Found %d new alerts for %s %s'%(len(alerts), self.domain, self.group))
				if self.test_mode is False:
					self.insert_alerts(alerts)
			del alerts
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
		argparser.add_argument('--domain', action='store', dest='domain', help='domain', default=None)
		argparser.add_argument('--test_mode', action='store_true', default=False)
		argparser.add_argument('--snooze', default = 30.0, type=float)
		args = argparser.parse_args()
		main(args)






