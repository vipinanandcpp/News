import re, signal, time, collections, os, gzip, re, pytz
from functools import wraps
from random import randrange
from dateutil import parser as dtparser
import requests
import justext
import xmltodict
#from mechanize import Browser
#from selenium import webdriver
from unidecode import unidecode
from bs4 import UnicodeDammit
from bson import json_util
#import traceback

user_agents = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:29.0) Gecko/20100101 Firefox/29.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.68 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.76.4 (KHTML, like Gecko) Version/7.0.4 Safari/537.76.4'
]

headers = {	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.52 Safari/537.36',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
			'Accept-Encoding': 'none',
			'Accept-Language': 'en-US,en;q=0.8',
			'Connection': 'keep-alive'}

bad_chars = frozenset([0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 14, 15, 16, 17,
				 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])

eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

def dictify(xml_response):
	r = xmltodict.parse(xml_response)
	j = json_util.dumps(r)
	return json_util.loads(j)

def to_datetime(datestring):
	if not datestring:
		return ''
	else:
		datestring = asciiize(datestring)

	try:
		date = dtparser.parse(datestring)
	except:
		date = datestring
	else:
		date = utc.localize(date).astimezone(eastern)
		try:
			date = date.astimezone(eastern)
		except:
			pass

	return date

def ckmkdirs(p):
	if not os.path.exists(p):
		os.makedirs(p)
	return p

def write_gzip(item, itempath, filename):
	ckmkdirs(itempath)
	with gzip.open(os.path.join(itempath, filename), 'wb') as f:
		f.write(json_util.dumps(item, indent=4, sort_keys=True))

def underscorize(s):
	s = re.sub(r'\W+', '_', s)
	return re.sub(r'_+', '_', s)

#http://stackoverflow.com/a/16176779/1599229
def flatten(l):
	for el in l:
		if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
			for sub in flatten(el):
				yield sub
		else:
			yield el


def unicodify(text, guess=None):
	''' returns unicoded version of text or empty unicode string '''
	if not text:
		return u""
	elif isinstance(text, unicode):
		return text
	elif isinstance(text, str):
		if guess:
			unic = UnicodeDammit(text, guess, smart_quotes_to="ascii").unicode_markup
		else:
			unic = UnicodeDammit(text).unicode_markup
		if unic:
			return unic
		else:
			try:
				return unicode(text.decode('ascii', 'ignore'))
			except:
				return u""

def try_to_unicodify(text, guess=None):
	if not text:
		return u""
	elif isinstance(text, unicode):
		return text
	elif isinstance(text, str):
		if guess:
			unic = UnicodeDammit(text, guess, smart_quotes_to="ascii").unicode_markup
		else:
			unic = UnicodeDammit(text).unicode_markup
		if unic:
			return unic
		else:
			try:
				return unicode(text.decode('ascii', 'ignore'))
			except:
				return text

def sanitize(text):
	if isinstance(text, basestring):
		text = "".join([" " if ord(char) in bad_chars else char for char in text])
		if isinstance(text, unicode):
			text = re.sub(r'[\u02bc\u2018\u2019\u201a\u201b\u2039\u203a\u300c\u300d]', "'", text)
			text = re.sub(r'[\u00ab\u00bb\u201c\u201d\u201e\u201f\u300e\u300f]', '"', text)
		return text
	else:
		return u''

def asciiize(text):
	return unidecode(try_to_unicodify(sanitize(text)))

def harvest_justext(html, justgood=False):
	texts = []
	rejected_texts = []
	paragraphs = justext.justext(unicodify(html), justext.get_stoplist("English"))
	for paragraph in paragraphs:
		if paragraph:
			if paragraph.is_boilerplate: rejected_texts.append(paragraph.text)
			else: texts.append(paragraph.text)
	if justgood:
		return texts
	return {"texts": texts, "rejected_texts": rejected_texts}

def extract_text(html):
	texts = []
	rejected_texts = []
	paragraphs = justext.justext(html, justext.get_stoplist("English"))
	for paragraph in paragraphs:
		if paragraph:
			t = sanitize(paragraph.text)
			if paragraph.is_boilerplate:
				rejected_texts.append(t)
			else:
				texts.append(t)
	return {"texts": texts, "rejected_texts": rejected_texts}

# need to pass in a requests response object
def extract_content_type(r):
	c = r.headers.get('content-type', "=").split("=")[1]
	if not c:
		c = 'UTF-8'
	return c

def get_content_type(pyq):
	meta = pyq('meta')
	for m in meta:
		a = m.attrib
		if a.get('charset'):
			return a['charset']
		elif a.get('http-equiv'):
			return a.get('content', 'c=utf-8').split("=")[1]
	return 'utf-8'

def get_encoding(soup):
	encod = soup.meta.get('charset')
	if encod == None:
		encod = soup.meta.get('content-type')
		if encod == None:
			content = soup.meta.get('content')
			match = re.search('charset=(.*)', content)
			if match:
				encod = match.group(1)
			else:
				#encod = chardet.detect()
				#raise ValueError('unable to find encoding')
				encod = "UTF-8"
	return encod

class Timeout():
	class Timeout(Exception):
		pass

	def __init__(self, sec):
		self.sec = sec

	def __enter__(self):
		signal.signal(signal.SIGALRM, self.raise_timeout)
		signal.alarm(self.sec)

	def __exit__(self, *args):
		signal.alarm(0) # disable alarm

	def raise_timeout(self, *args):
		raise Timeout.Timeout()

def retry(ExceptionToCheck, tries=3, delay=30, backoff=2, logger=None, timeout_length=60):
	def deco_retry(f):
		@wraps(f)
		def f_retry(*args, **kwargs):
			mtries, mdelay = tries, delay
			while mtries > 1:
				try:
					with Timeout(timeout_length):
						return f(*args, **kwargs)
				except ExceptionToCheck as e:
					msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
					if logger:
						logger.warning(msg)
					else:
						print (msg)
					time.sleep(mdelay)
					mtries -= 1
					mdelay *= backoff
			return f(*args, **kwargs)
		return f_retry  # true decorator
	return deco_retry

@retry(Exception, tries=3)
def retry_request(url, params={}):
	return requests.get(url, params=params, verify=False, timeout=60)

@retry(Exception, tries=3, delay=30, backoff=2, logger=None, timeout_length=600)
def requests_dot_get(url, headers=headers):
	return requests.get(url, headers=headers, verify=False, timeout=600)

# def mechanize_to_text(url):
# 	br = Browser()
# 	br.set_handle_robots(False)
# 	br.set_handle_equiv(False)
# 	br.set_handle_refresh(False)
# 	br.addheaders = [('User-agent', user_agents[randrange(0,3)])]
# 	response = br.open(url)
# 	html = response.read()
# 	result = harvest_justext(html)
# 	result['title'] = br.title()
# 	return result

def phantom_to_text(url):
	driver = webdriver.PhantomJS()
	driver.get(url)
	html = driver.page_source
	result = harvest_justext(html)
	result['title'] = driver.title
	return result

def requests_helper(url):
	return requests_dot_get(url)
