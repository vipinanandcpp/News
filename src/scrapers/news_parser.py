import sys, datetime, time, re, threading, uuid, os
import requests, pymongo
from requests import ConnectionError
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
from requests_futures.sessions import FuturesSession
from data_processing_pipeline.write_file import WriteFile
from Utilities import openMongoDBConnection, get_redis_connection0, utc, eastern
import traceback
import abc
import settings

try:
# For Python 3.0 and later
	import queue as Queue
	from requests.packages.urllib3.exceptions import InsecureRequestWarning
	requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
# Fall back to Python 2's urllib2
	import Queue as Queue

redis_connection = get_redis_connection0()
mongodb_connection = openMongoDBConnection(os.path.join(settings.database_directory, 'database.config'), 'mongoDB')


strip_html = lambda data:re.compile(r'<.*?>').sub('', data).encode('ascii', 'ignore').replace('<![CDATA[', '').replace(']]>', '')
dummy_ticker = 'NEWS_HEADLINE'

class NewsParsers(object):
	__metaclass__ = abc.ABCMeta
	def __init__(self, domain, group='newswires', test_mode=False, timeout = 30, max_workers = 50, parse_articles = False, run_history=False, expire_redis_keys = False):
		self.domain = domain
		self.run_history = run_history
		self.parse_articles = parse_articles
		self.queue = Queue.Queue()
		self.group = group
		self.test_mode = test_mode
		self.session0 = requests.Session()
		self.expire_redis_keys = expire_redis_keys
		self.write_file = WriteFile(group=self.group, filename=self.domain)
		self.session = FuturesSession(executor=ThreadPoolExecutor(max_workers=max_workers))
		self.cache = set() if self.test_mode is True else self.update_cache()
		self.timeout = timeout
		self.db = mongodb_connection['news']
		if self.parse_articles == False:
			sys.stdout.write('\n Parsing articles is disallowed using listener for %s\n' %(self.domain))
		else:
			self.listener = Listener(self)
			self.listener.start()

	def __del__(self):
		sys.stdout.write('\n Creating index for %s %s\n'%(self.group, self.domain))
		self.db[self.group][self.domain].create_index('id', unique=True, background=True)
		self.db[self.group][self.domain].create_index('url', unique=True, background = True)
		self.db[self.group][self.domain].create_index('tickers', background = True)
		self.db[self.group][self.domain].create_index('timestamp', background = True)

	@abc.abstractmethod
	def parse_start_page(self):
		pass

	def update_cache(self):
		_cache = redis_connection.smembers(self.domain + "_CACHE")
		_cache = set() if _cache is None else set(_cache)
		sys.stdout.write('\tloaded %d items from the cache' % len(_cache))
		return _cache

	def pull_articles(self, alerts, timeout = 180):
		sys.stdout.write('\nentering pull articles function for %s / %s @ %s' % (self.domain, self.group, str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
		futures = {self.session.get(alert['url']) : alert['url'] for alert in alerts}
		results = {}
		for future in cf.as_completed(futures, timeout=timeout):
			try:
				if future.result().status_code==200:
					url = futures.get(future)
					results[url] = future.result()
					sys.stdout.write('\nArticle Response obtained for %s / %s future url %s @ %s\n' % (self.domain, self.group, futures.get(future), str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
				else:
					sys.stderr.write('\nResponse code %d found for future url %s @ %s\n' % (future.result().status_code, futures.get(future), str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
			except ConnectionError as e:
				sys.stderr.write('\nConnection error while pulling articles for %s @ %s' % (self.domain, str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
				break
			except Exception as e:
				sys.stderr.write('\nError pulling articles -> %s %s %s\n' % (str(e),self.domain, str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
		return results

	def process_articles(self, alerts, articles):
		if len(alerts):
			sys.stdout.write('\nProcessing articles for %d alerts and %d articles \n' %(len(alerts), len(articles)))
		return [self.process_article(alert, articles[alert['url']]) for alert in alerts if alert['url'] in articles]

	@abc.abstractmethod
	def process_article(self, alert, article):
		pass

	def push(self, alerts):
		assert isinstance(alerts, list)
		try:
			if len(alerts):
				self.db[self.group][self.domain].insert(alerts, w=0)
		except pymongo.errors.ServerSelectionTimeoutError as e:
			sys.stderr.write('\n\t' + str(traceback.print_exc()))
			sys.exit(1)


	def insert_alerts(self, alerts, expiry=30):
		self.push(alerts) #mongo insertion
		if self.test_mode == False:
			pipe = redis_connection.pipeline(False)
			if len(alerts):
				for alert in alerts:
					self.cache.add(alert['id'])
					pipe.sadd(self.domain + "_CACHE", alert['id'])
					if self.expire_redis_keys:
						pipe.expire(self.domain + "_CACHE", datetime.timedelta(days=expiry))
				pipe.execute()
				self.write_file.process_messages(alerts)

	def runner(self, snooze=10):
		sys.stdout.write('\n' + self.domain + ' parser starting @ %s' % str(datetime.datetime.now()))
		self.update_cache()
		while(True):
			try:
				sys.stdout.write("\ncalling @ %s" % str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern)))

				if self.parse_articles == True:
					for alert in self.parse_start_page():
						self.queue.put(alert)
				else:
					alerts = self.parse_start_page()
					for alert in alerts:
						if 'tickers' not in alert:
							alert.setdefault('tickers', [dummy_ticker])
						elif len(alert['tickers']) == 0:
							alert.setdefault('tickers', [dummy_ticker])
						if ('article' not in alert):
							alert.setdefault('article', '')
						elif len(alert['article']) == 0:
							alert.setdefault('article', '')
					self.insert_alerts(alerts)
				if self.run_history:
					break
			except Exception as e:
				sys.stderr.write('\nController Error -> %s @ %s' % (str(e), datetime.datetime.now()))
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
			finally:
				sys.stdout.flush()
				time.sleep(snooze)

class Listener(threading.Thread):
	def __init__(self, news_parser_instance):
		threading.Thread.__init__(self)
		self.news_parser_instance = news_parser_instance
		self.kill_thread_event = False

	def pull_articles_worker_thread(self, thread_id, _alerts):
		start_time = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)
		_articles = {}
		sys.stdout.write('\n executing thread %s for %s @ %s' % (thread_id, self.news_parser_instance.domain, str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))))
		while len(_articles) == 0:
			current_time = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)
			if (current_time - start_time).seconds >= 60:
				sys.stdout.write('\nThread time out %s @ %s for %s from %s\n' % (thread_id, str(current_time), self.news_parser_instance.domain, str(start_time)))
				break
			_articles = self.news_parser_instance.pull_articles(_alerts, timeout = self.news_parser_instance.timeout)
		if len(_articles):
			sys.stdout.write('\nextracted %d alerts\t%d articles @ %s for %s\n' % (len(_alerts), len(_articles), str(utc.localize(datetime.datetime.utcnow()).astimezone(eastern)), self.news_parser_instance.domain))
			alerts = self.news_parser_instance.process_articles(_alerts, _articles)
			self.news_parser_instance.insert_alerts(start_time, alerts)

	def run(self):
		while True:
			if self.kill_thread_event == True:
				sys.stdout.write('\n Stopping articles listener thread for %s' %(self.news_parser_instance.domain))
				break
			_alerts = []
			while not self.news_parser_instance.queue.empty():
				_alerts.append(self.news_parser_instance.queue.get())
				self.news_parser_instance.queue.task_done()

			if len(_alerts):
				thread_id = uuid.uuid4().hex
				worker = threading.Thread(name="articles_fetcher_"+str(thread_id), target=self.pull_articles_worker_thread, args=(str(thread_id), _alerts))
				worker.setDaemon(True)
				worker.start()

	# broken
	def stop(self):
		pass
