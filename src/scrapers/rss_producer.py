import sys, argparse, os, time, settings, signal, atexit, threading
from pika_publisher import PIKA_PUBLISHER
from url_processor import URLProcessor
from bson import json_util
from scrapers.news_parser import NewsParsers, redis_connection
from Utilities import hostname0, python_version
import rss_producer_rules
import logging

if python_version >= 3:
	import feedparser
else:
	try:
		import feedparser2 as feedparser
	except ImportError:
		import feedparser

class RSS_PRODUCER(NewsParsers):
	pika_publisher = None
	stop_runner = False

	@staticmethod
	def start_producer():
		if RSS_PRODUCER.pika_publisher is None:
			try:
				RSS_PRODUCER.pika_publisher = PIKA_PUBLISHER(amqp_url = "amqp://%s"%(hostname0), exchange_name = 'rss', routing_key = 'rss', queue_name = 'rss', exchange_type = 'direct')
				RSS_PRODUCER.pika_publisher.run()
			except Exception as e:
				sys.stderr.write('Error connecting to RabbitMQ\n -> %s'%(e))
				RSS_PRODUCER.stop_producer()

	@staticmethod
	def stop_rss_producer(signum = None, frame = None):
		RSS_PRODUCER.stop_runner = True

	@staticmethod
	def stop_producer(signum = None, frame = None):
		if RSS_PRODUCER.pika_publisher is not None:
			sys.stdout.write("Shutting down RSS producer channel\n")
			RSS_PRODUCER.pika_publisher.stop()
			RSS_PRODUCER.pika_publisher = None
			RSS_PRODUCER.stop_runner = True


	def __init__(self, domain, group='rss',parse_articles = False, test_mode = False, timeout = 600, max_workers = 10, expire_redis_keys = False, start_producer=False):
		super(RSS_PRODUCER, self).__init__(domain= domain, group=group, test_mode=test_mode, timeout=timeout, parse_articles=parse_articles, max_workers = max_workers, expire_redis_keys = expire_redis_keys)
		self.url_processor = URLProcessor(max_workers = max_workers)
		self.rss_producer_rule_instance = getattr(rss_producer_rules, self.domain)()
		self.start_producer = start_producer

	def __del__(self):
		super(RSS_PRODUCER, self).__del__()

	def process_article(self, alert, article):
		return alert

	def parse_start_page(self, url):
		alerts = []
		data = RSS_PRODUCER.call_feedparser(url)
		if hasattr(data, 'status'):
			if data.status != 304:
				articles = data.get('entries', [])
				if len(articles):
					alerts.extend(self.rss_producer_rule_instance.parse_feed(self, articles))
		else:
			sys.stderr.write("No data found for url %s\n"%(url))
		return alerts

	def runner(self, snooze=10):
		if self.start_producer:
			while RSS_PRODUCER.pika_publisher is None:
				pass
		sys.stdout.write('\nProcessing RSS runner for %s\n'%(self.domain))
		while not RSS_PRODUCER.stop_runner:
			alerts = []
			for source_url in self.source_urls:
				__alerts = []
				for alert in self.parse_start_page(source_url):
					if alert['id'] not in self.cache:
						alerts.append(alert)
						__alerts.append(alert)
				if self.test_mode is False:
					if len(__alerts):
						if self.start_producer:
							if source_url in self.scheduled_source_urls:
								if RSS_PRODUCER.pika_publisher.is_running:
									RSS_PRODUCER.pika_publisher.publish_messages(__alerts)
								else:
									sys.stderr.write('RSS producer is not running \n')
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
		if hasattr(d, 'status'):
			if d.status == 200:
			 	if hasattr(d, 'modified'):
			 		redis_connection.set(url + "_modified", d.modified)
			 	if hasattr(d, 'etag'):
			 		redis_connection.set(url + "_etag", d.etag)
		else:
			sys.stderr.write("No data found for url %s\n"%(url))
		return d

def main(args):
	test_mode = args.test_mode
	snooze = args.snooze
	start_producer = args.start_producer
	metadata = {}
	with open('rss_feeds.json') as fp:
		metadata = json_util.loads(fp.read())
	if len(metadata):
		for domain, domain_metadata in metadata.items():
			if domain_metadata.get('active', False) is True:
				logging.basicConfig(filename=os.path.join(settings.log_directory,domain+'_error.log'),level=logging.ERROR, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z')
				rss_producer = RSS_PRODUCER(domain=domain, test_mode=test_mode, start_producer=start_producer)
				rss_producer.base_url = domain_metadata['base_url']
				rss_producer.source_urls = domain_metadata['source_urls']
				rss_producer.scheduled_source_urls = [source_url for source_url, is_scheduled in zip(domain_metadata['source_urls'], domain_metadata['scheduled']) if is_scheduled] if len(domain_metadata.get('scheduled',[])) else []
				threading.Thread(target=rss_producer.runner, args = (snooze, )).start()

	if start_producer:
		for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
			signal.signal(sig, RSS_PRODUCER.stop_producer)
		atexit.register(RSS_PRODUCER.stop_producer)
		RSS_PRODUCER.start_producer()
	else:
		for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
			signal.signal(sig, RSS_PRODUCER.stop_rss_producer)
		atexit.register(RSS_PRODUCER.stop_rss_producer)
		while not RSS_PRODUCER.stop_runner:
			pass

if __name__== '__main__':
	try:
		__IPYTHON__
		sys.stdout.write('\nrunning via ipython -> not running continously')
	except NameError:
		argparser = argparse.ArgumentParser(description='RSS producer')
		argparser.add_argument('--start_producer', action='store_true', default=False)
		argparser.add_argument('--test_mode', action='store_true', default=False)
		argparser.add_argument('--snooze', default = 30.0, type=float)
		args = argparser.parse_args()
		main(args)
