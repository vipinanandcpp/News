import sys, logging, traceback, requests
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
from newspaper import Article

headers = {	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.52 Safari/537.36',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
			'Accept-Encoding': 'none',
			'Accept-Language': 'en-US,en;q=0.8',
			'Connection': 'keep-alive'}

class URLProcessor(object):

	def __init__(self, max_workers=50):
		self.max_workers = max_workers

	@staticmethod
	def run_extraction(url, language='en'):
		extracted = None
		try:
			r = requests.get(url, headers=headers, verify=False, timeout=600)
			extracted = Article(url = url, language=language)
			extracted.download(html=r.content.decode("utf-8"))
			extracted.parse()
			extracted.nlp()
		except Exception as e:
			sys.stderr.write('\n\t' + str(traceback.print_exc()))
			logging.error(e, exc_info=1)
			extracted = None
		return extracted

	def process_url(self, url, language='en'):
		return URLProcessor.run_extraction(url, language=language)

	def process_urls(self, urls, timeout = 600, language = 'en'):
		results = {}
		with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
			futures = {executor.submit(self.process_url, url, language=language):url for url in urls}
			for future in cf.as_completed(futures, timeout=timeout):
				if future.result() is not None:
					url = futures[future]
					try:
						results[url] = future.result()
					except Exception as e:
						sys.stderr.write('\n\t' + str(traceback.print_exc()))
						logging.error(e, exc_info=1)
		return results
