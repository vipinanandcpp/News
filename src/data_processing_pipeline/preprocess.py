# -*- coding: utf-8 -*-
import nltk
import pytz, re, datetime, sys, traceback
import string
from dateutil import parser
import pandas as pd
from text_mining.tools import unicodify, sanitize
from data_processing_pipeline import harvard_sentiment as sentiment
from data_processing_pipeline.stoplists import stop
# twokenize: https://github.com/myleott/ark-twokenize-py
from twokenize import tokenizeRawTweetText as twokenize
from unidecode import unidecode
from nltk.stem import SnowballStemmer

date_format = "%a %b %d %H:%M:%S %Y"
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

url_matcher = re.compile(ur'(https?://[\S]+)')

def process_tweet_display(line, harvard_sentiment=False, geodata=False):
	try:
		message = {}
		message['created_at'] = parser.parse(line['created_at'])#.astimezone(eastern)
		message['id'] = line['id_str']
		message['screen_name'] = line['user']['screen_name']
		message['name'] = line['user']['name']
		message['text'] = line['text']
		message['symbols'] = sorted(set([ x['text'].upper() for x in line['entities']['symbols'] ]))
		message['avatar'] = line['user']['profile_image_url_https']
		if 'retweeted_status' in line:
			message['RT_created_at'] = parser.parse(line['retweeted_status']['created_at'])
			message['RT_id'] = line['retweeted_status']['id_str']
			message['RT_screen_name'] = line['retweeted_status']['user']['screen_name']
			message['RT_text'] = line['retweeted_status']['text']
		for media_object in line['entities'].get('media', []):
			if media_object.get('type', '') == 'photo':
				message['photo'] = {}
				message['photo']['url'] = media_object['media_url_https']
				message['photo']['h'] = media_object.get('sizes', {}).get('large', {}).get('h', {})
				message['photo']['w'] = media_object.get('sizes', {}).get('large', {}).get('w', {})
		for url_object in line['entities'].get('urls', []):
			if 'expanded_url' in url_object:
				if 'https://amp.twimg.com/v' in url_object['expanded_url']:
					message['video_url'] = url_object['expanded_url']
				elif 'https://vine.co/v' in url_object['expanded_url']:
					message['vine_url'] = url_object['expanded_url']
		if geodata:
			message['coordinates'] = None if line['coordinates'] is None else line['coordinates']['coordinates']
		if harvard_sentiment:
			positive_tokens, negative_tokens = sentiment.calculate_score(line['text'])
			message['sentiment'] = len(positive_tokens) - len(negative_tokens)
			if geodata:
				message['RT_coordinates'] = None if line['retweeted_status']['coordinates'] is None else line['retweeted_status']['coordinates']['coordinates']
		return message
	except Exception as e:
		print 'Error processing line %s -> %s' % (line, e.message)

def process_tweet_short(line, harvard_sentiment=False):
	try:
		message = {}
		message['created_at'] = line['created_at'] if isinstance(line['created_at'], datetime.datetime) else parser.parse(line['created_at'])#.astimezone(eastern)
		message['id'] = line['id_str']
		message['screen_name'] = line['user']['screen_name']
		message['text'] = line['text']
		message['symbols'] = sorted(set([ x['text'].upper() for x in line['entities']['symbols'] ]))
		message['hashtags'] = sorted(set([x['text'].upper() for x in line.get('entities', {}).get('hashtags', [])]))
		message['user_mentions'] = [ x['screen_name'] for x in line['entities']['user_mentions'] ]
		message['retweet_count'] = line['retweet_count']	# how many times was *this* tweet retweeted @
		message['favorite_count'] = line['favorite_count']	# how many times was *this* tweet favorited @
		message['URLs'] = [url_object['expanded_url'] for url_object in line['entities'].get('urls', [])]
		message['URLs'] = message['URLs'] if len(message['URLs'])>0 else url_matcher.findall(message['text'])
		message['user_data'] = {
			'name' : line['user']['name'],
			'description' : line['user']['description'],
			'location' : line['user']['location'],
			'followers_count' : line['user']['followers_count'],
			'friends_count' : line['user']['friends_count'],
			'listed_count' : line['user']['listed_count'],
			'statuses_count' : line['user']['statuses_count'],
			'created_at' : line['user']['created_at'],
			'avatar': line['user']['profile_image_url_https']
		}
		# if harvard_sentiment:
		# 	positive_tokens, negative_tokens = sentiment.calculate_score(line['text'])
		# 	message['sentiment'] = len(positive_tokens) - len(negative_tokens)
		message['coordinates'] = None if line['coordinates'] is None else line['coordinates']['coordinates']

		message['tags'] = []
		if len(message['user_mentions']):
			message['tags'].append('conversation')

		if line['text'].startswith("@"):
			message['tags'].append('response')

		if 'retweeted_status' in line:	# is this a retweet, i.e. did someone else create this tweet?
			message['RT_created_at'] = parser.parse(line['retweeted_status']['created_at'])
			message['RT_id'] = line['retweeted_status']['id_str']
			message['RT_screen_name'] = line['retweeted_status']['user']['screen_name']
			message['RT_text'] = line['retweeted_status']['text']
			message['RT_coordinates'] = None if line['retweeted_status']['coordinates'] is None else line['retweeted_status']['coordinates']['coordinates']
			message['tags'].append('retweet')
			message['RT_user_data'] = {
				'name' : line['retweeted_status']['user']['name'],
				'description' : line['retweeted_status']['user']['description'],
				'location' : line['retweeted_status']['user']['location'],
				'followers_count' : line['retweeted_status']['user']['followers_count'],
				'friends_count' : line['retweeted_status']['user']['friends_count'],
				'listed_count' : line['retweeted_status']['user']['listed_count'],
				'statuses_count' : line['retweeted_status']['user']['statuses_count'],
				'created_at' : line['retweeted_status']['user']['created_at'],
				'avatar': line['retweeted_status']['user']['profile_image_url_https']
			}
		return message
	except Exception as e:
		sys.stderr.write('\nError processing line %s -> %s' % (line, e.message))
		sys.stderr.write('\n\t' + str(traceback.print_exc()))

def process_tweet(line):
	try:
		message = {}
		user = {}
		message['created_at'] = line['created_at'] if isinstance(line['created_at'], datetime.datetime) else parser.parse(line['created_at'])#.astimezone(eastern)
		message['id'] = line['id_str']
		message['user_id'] = line['user']['id_str']
		message['name'] = line['user']['name']
		message['lang'] = line['lang']
		message['screen_name'] = line['user']['screen_name']
		message['location'] = line['user']['location']
		message['time_zone'] = line['user']['time_zone']
		message['text'] = line['text']
		message['URLs'] = [ x['expanded_url'] for x in line['entities']['urls'] ]
		message['URLs'] = message['URLs'] if len(message['URLs'])>0 else url_matcher.findall(message['text'])
		message['hashtags'] = [ x['text'] for x in line['entities']['hashtags'] ]
		message['symbols'] = sorted(set([ x['text'].upper() for x in line['entities']['symbols'] ]))
		message['user_mentions'] = [ {'screen_name':x['screen_name'], 'id_str':x['id_str']} for x in line['entities']['user_mentions'] ]
		message['user_url'] = line['user']['url']
		message['description'] = line['user']['description']
		message['followers_count'] = line['user']['followers_count']
		message['friends_count'] = line['user']['friends_count']
		message['listed_count'] = line['user']['listed_count']
		message['user_created_at'] = line['user']['created_at']
		message['statuses_count'] = line['user']['statuses_count']
		message['retweet_count'] = line['retweet_count']
		message['favorite_count'] = line['favorite_count']
		message['coordinates'] = None if line['coordinates'] is None else line['coordinates']['coordinates']
		message['place'] = None if line['place'] is None else line['place']['bounding_box']['coordinates']
		message['source'] = line['source']
		message['RT'] = 'retweeted_status' in line
		positive_tokens, negative_tokens = sentiment.calculate_score(line['text'])
		message['positive_tokens'] = positive_tokens
		message['positive_sentiment'] = len(positive_tokens)
		message['negative_tokens'] = negative_tokens
		message['negative_sentiment'] = len(negative_tokens)
		message['sentiment'] = len(positive_tokens) - len(negative_tokens)
		message['in_reply_to_screen_name'] = line['in_reply_to_screen_name']
		message['in_reply_to_user_id_str'] = line['in_reply_to_user_id_str']

		if 'retweeted_status' in line:
			message['RT_text'] = line['retweeted_status']['text']
			message['RT_id'] = line['retweeted_status']['id_str']
			message['RT_favorite_count'] = line['retweeted_status']['favorite_count']
			message['RT_source'] = line['retweeted_status']['source']
			message['RT_retweet_count'] = line['retweeted_status']['retweet_count']
			message['RT_created_at'] = parser.parse(line['retweeted_status']['created_at'])
			message['RT_lang'] = line['retweeted_status']['lang']
			message['RT_coordinates'] = None if line['retweeted_status']['coordinates'] is None else line['retweeted_status']['coordinates']['coordinates']
			message['RT_screen_name'] = line['retweeted_status']['user']['screen_name']
			message['RT_user_id'] = line['retweeted_status']['user']['id_str']
			message['RT_description'] = line['retweeted_status']['user']['description']
			message['RT_followers_count'] = line['retweeted_status']['user']['followers_count']
			message['RT_friends_count'] = line['retweeted_status']['user']['friends_count']
			message['RT_location'] = line['retweeted_status']['user']['location']
			message['RT_in_reply_to_screen_name'] = line['retweeted_status']['in_reply_to_screen_name']
			message['RT_in_reply_to_user_id_str'] = line['retweeted_status']['in_reply_to_user_id_str']
			message['RT_in_reply_to_status_id'] = line['retweeted_status']['in_reply_to_status_id']
			message['RT_URLs'] = [ x['expanded_url'] for x in line['retweeted_status']['entities']['urls'] ]
			message['RT_hashtags'] = [ x['text'] for x in line['retweeted_status']['entities']['hashtags'] ]
			message['RT_symbols'] = [ x['text'].upper() for x in line['retweeted_status']['entities']['symbols'] ]
			message['RT_user_mentions'] = [ {'screen_name':x['screen_name'], 'id_str':x['id_str']} for x in line['retweeted_status']['entities']['user_mentions'] ]
			positive_tokens, negative_tokens = sentiment.calculate_score( line['retweeted_status']['text'] )
			message['RT_positive_tokens'] = positive_tokens
			message['RT_negative_tokens'] = negative_tokens

		if 'user' in line:
			user = {
				'created_at': parser.parse(line['user']['created_at']),#.astimezone(eastern),
				'description':line['user']['description'],
				'favourites_count':line['user']['favourites_count'],
				'friends_count':line['user']['friends_count'],
				'followers_count':line['user']['followers_count'],
				'location':line['user']['location'],
				'id':line['user']['id_str'],
				'lang':line['user']['lang'],
				'time_zone':line['user']['time_zone'],
				'name':line['user']['name'],
				'screen_name':line['user']['screen_name'],
				'listed_count':line['user']['listed_count'],
				'statuses_count':line['user']['statuses_count'],
				'profile_image_url_https':line['user']['profile_image_url_https'],
				'verified':line['user']['verified']
			}
		return message, user
	except Exception as e:
		print 'Error processing line %s -> %s' % (line, e.message)

def process_stocktwits(m):
	message = {}
	message['id'] = m['id']
	message['created_at'] = parser.parse( m['created_at'] ).astimezone(eastern)
	message['body'] = m['body']
	message['username'] = m['user']['username']
	message['positive'] = list(sentiment.calculate_score(m['body'])[0])
	message['negative'] = list(sentiment.calculate_score(m['body'])[1])
	message['tickers'] = [ticker for symbol_group in m.get('symbols', []) for ticker in symbol_group['symbol']]
	return message

def build_dataframe(messages):
	df = pd.DataFrame(messages).sort('created_at', ascending=True)[ [ 'created_at', 'text', 'id', 'RT', 'screen_name', 'name', 'user_id', 'location', 'description', 'URLs', 'positive_tokens', 'negative_tokens','sentiment' ]]
	df = df.sort('created_at', ascending=True)
	df['datetime'] = df['created_at'].map(lambda x: x.replace(second=0, microsecond=0))
	return df.set_index('datetime')

def resample_dataframe(df, sample_frequency='1min'):
	return df.resample(sample_frequency, how={'id':len, 'positive_tokens': sum, 'negative_tokens':sum,'screen_name':lambda x: x.tolist(), 'text':lambda x: x.tolist(), 'sentiment':sum})

def process_message(text, url_marker='AT_URL', user_marker='AT_USER', cashtag_marker='AT_CASHTAG', number_marker='AT_NUMBER', remove_rt_marker=True, check_numbers=True):
	if remove_rt_marker and text.startswith('RT'):
		text = text.split('RT')[1]

	text = text.lower().replace(u"\u2018", "").replace(u"\u2019", "").replace(u"\u201c","").replace(u"\u201d", "")
	# text = text.lower()
	text = re.sub('((www\.[\s]+)|(https?://[^\s]+))', url_marker, text)	# Convert www.* or https?://* to URL
	text = re.sub('@[^\s]+', user_marker, text)	# Convert @username to AT_USER
	text = re.sub('[\s]+', ' ', text)	# Remove additional white spaces
	text = re.sub(r'#([^\s]+)', r'\1', text)	# Replace #word with word
	text = re.sub(r'\$[A-Z]{1,4}', cashtag_marker, text)	# Replace $cashtags
	# percentages 3.4%
	# dollar amounts
	# decimal values
	# text = re.sub(r'\$([^\s]+)', cashtag_marker, text)	# Replace $cashtags
	if check_numbers:
		text = re.sub(r'\d+', number_marker, text)	# Replace numbers
	text = text.strip('\'"')	#trim
	return text.strip()	#h.unescape( text ).encode('utf-8')


# need to handle:
# ,	=> remove
# 's => remove
# - => BE CAREFUL!!!, only remove if it separate non-numerical characters
# . => BE CAREFUL!!!, only remove if it separate non-numerical characters
# : => remove
# be careful with the order of parsing!!!
# should we compile the regex?
def process_message_modified(text, url_marker='AT_URL', user_marker='AT_USER', cashtag_marker='AT_CASHTAG', number_marker='AT_NUMBER', percentage_marker='AT_PERCENTAGE', currency_marker='AT_CURRENCY', decimal_marker='AT_DECIMAL', date_marker='AT_DATE', quarter_marker='AT_QUARTER', year_marker='AT_YEAR', remove_rt_marker=False):
	if remove_rt_marker and text.startswith('RT'):
		text = text.split('RT')[1]

	text = text.lower().replace(u"\u2018", "").replace(u"\u2019", "").replace(u"\u201c","").replace(u"\u201d", "")
	text = re.sub('((www\.[\s]+)|(https?://[^\s]+))', url_marker, text)	# Convert www.* or https?://* to URL
	text = re.sub('@[^\s]+', user_marker, text)	# Convert @username to AT_USER
	text = re.sub(",", " ", text)	# remove commas
	text = re.sub(r"\'s", "", text)	# remove possession
	text = re.sub(ur'(\D)-(\D)', '\\1\\2', text)	# only removes "-" between two characters, meant to preserve "in-line" but can be dangerous!!!!!!!!!!
	text = re.sub('[\s]+', ' ', text)	# Remove additional white spaces
	text = re.sub(r'20\d\d', year_marker, text)
	text = re.sub(r'\d\d\s(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s\d{4}', date_marker, text)
	text = re.sub(r'q\d\s', quarter_marker, text)
	text = re.sub(r'#([^\s]+)', r'\1', text)	# Replace #word with word
	text = re.sub("\<[A-Za-z]{1,5}.[A-Za-z]{1,4}\>", cashtag_marker, text).strip()	# Reuters tickers
	text = re.sub(r'\$[A-Za-z]{1,4}', cashtag_marker, text)	# Replace $cashtags
	text = re.sub(r'\d+.\d+%', percentage_marker, text)	# Replace percentages
	text = re.sub(r'\$\d+.\d+', currency_marker, text)	# Replace percentages
	text = re.sub(r'\d+.\d+', decimal_marker, text)	# Replace decimals
	text = re.sub(r'\d+', number_marker, text)	# Replace numbers
	text = re.sub('\s+', ' ', text).strip()	# normalize spaces
	text = text.strip('\'"')	#trim
	return text.strip()	#h.unescape( text ).encode('utf-8')

percentage = re.compile(r'\(?(\+|-)?([0-9]{1,4})?(\.)([0-9]{0,4})(\%)(?: ?pre)?\)?')
def pm_modified(text, url_marker='AT_URL', user_marker='AT_USER', cashtag_marker='AT_CASHTAG', number_marker='AT_NUMBER', percentage_marker='AT_PERCENTAGE', currency_marker='AT_CURRENCY', decimal_marker='AT_DECIMAL', date_marker='AT_DATE', quarter_marker='AT_QUARTER', year_marker='AT_YEAR', month_marker='AT_MONTH', remove_rt_marker=False):
	if remove_rt_marker and text.startswith('RT'):
		text = text.split('RT')[1]
	text = re.sub('\s+', ' ', text).strip()	# normalize spaces
	text = text.lower().replace(u"\u2018", "").replace(u"\u2019", "").replace(u"\u201c","").replace(u"\u201d", "")
	text = re.sub('((www\.[\s]+)|(https?://[^\s]+))', url_marker, text)	# Convert www.* or https?://* to URL
	text = re.sub('@[^\s]+', user_marker, text)	# Convert @username to AT_USER
	text = re.sub(",", " ", text)	# remove commas
	text = re.sub(r"\'s", "", text)	# remove possession
	text = re.sub(ur'(\D)-(\D)', '\\1\\2', text)	# only removes "-" between two characters, meant to preserve "in-line" but can be dangerous!!!!!!!!!!
	text = re.sub('[\s]+', ' ', text)	# Remove additional white spaces

	text = re.sub(r'20\d\d', year_marker, text)
	text = re.sub(r'\d\d\s(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s\d{4}', date_marker, text)
	# text = re.sub(r'\s?\(jan|feb|march|mar|april|apr|may|jun|june|jul|july|aug|sep|sept|oct|nov|dec\\s?)', month_marker, text)
	text = re.sub(r'q\d\s', quarter_marker, text)
	text = re.sub(r'#([^\s]+)', r'\1', text)	# Replace #word with word
	text = re.sub(r'\$[A-Za-z]{1,4}', cashtag_marker, text)	# Replace $cashtags
	text = re.sub("\<[A-Za-z]{1,5}.[A-Za-z]{1,4}\>", cashtag_marker, text).strip()	# Reuters tickers
	#text = re.sub(r'\d+.\d+%', 'AT_PERCENTAGE', text)	# Replace percentages
	text = percentage.sub(lambda m:'AT_NEGATIVE_PERCENTAGE' if m.group(1) == '-' else 'AT_POSITIVE_PERCENTAGE', text)
	text = re.sub(r'\$\d+.\d+', currency_marker, text)	# Replace percentages
	text = re.sub(r'\d+.\d+', decimal_marker, text)	# Replace decimals
	text = re.sub(r'\d+', number_marker, text)	# Replace numbers
	text = text.replace(" unch ", ' AT_UNCH ').replace(' flat ', ' AT_UNCH ').replace(' halt ', ' AT_HALT ')
	text = text.strip('\'"')	#trim
	return text.strip()	#h.unescape( text ).encode('utf-8')


def process_preserve_message(text):
	text = re.sub('[\s]+', ' ', text)
	text = re.sub(r'@([^\s]+)', r'\1', text)
	text = re.sub(r'#([^\s]+)', r'\1', text)
	text = re.sub(r'$([^\s]+)', r'\1', text)
	text = re.sub('((www\.[\s]+)|(https?://[^\s]+))', "", text)
	text = re.sub(r'\d+', "", text)
	text = text.strip('\'"')
	return text.encode('utf-8')

def strip_urls(text, url_marker=''):
	return re.sub('((www\.[\s]+)|(https?://[^\s]+))', url_marker, text).strip()

def preprocess_text(text, degree='heavy'):
	''' heavy = text to unicode with control and non-ascii chars removed
		moderate = text to unicode with control chars removed
		light = text to unicide '''
	unic = unicodify(text)
	if degree == 'moderate':
		unic = sanitize(unic)
	# utterly and completely removes non-ascii chars, replacing much more gracefully than the library version
	elif degree == 'heavy':
		unic = unidecode(sanitize(unic))
	return unic

def superprocess_text(text):
	unic = preprocess_text(text)
	return process_message(unic.lower(), '', '', '', '')


# double-punct and emoticons
repeated_non_word_char = re.compile(ur'^[^\w]{2,}$')
# aaaaaaaa  ,,,,,, etc
repeats = re.compile(ur'^(.)\1+$')

def check_tok(tok):
	if len(tok) > 1 and tok[0:4] != 'http' and not repeated_non_word_char.match(tok) and not tok in stop:
		return True
	return False

def tokenize(text):
	return twokenize(text.lower())

def tokenize_nopunct(text):
	return [tok.strip() if tok not in string.punctuation and not repeated_non_word_char.match(tok) else 'UNK' for tok in tokenize(text)]

snowball = SnowballStemmer("english", ignore_stopwords=False)
def snowballize(text):
	return [snowball.stem(tok) for tok in tokenize(text)]

def snowballize_max(text):
	return [snowball.stem(tok) if check_tok(tok) else 'UNK' for tok in tokenize(text)]

def tokenize_max(text):
	return [tok.strip() if check_tok(tok) else 'UNK' for tok in tokenize(text)]

def clean_tokens_nopunct_nostopwords(text):
	return [tok.strip() for tok in tokenize_nopunct(text) if (check_tok(tok) and tok is not 'UNK')]

def find_features_from_POS(features_list,*pos_args):
	pos = nltk.pos_tag(features_list)
	pos_filter_dict = {}
	if(len(pos_args) > 0):
		for arg in pos_args:
			pos_filter_dict[arg] = 1
		filtered_features_list = []
		for i in xrange(len(pos)):
			if pos_filter_dict.has_key(pos[i][1]):
				filtered_features_list.append(pos[i][0])
	else:
		return features_list
	return filtered_features_list


# {u'contributors': None,
#  u'coordinates': None,
#  u'created_at': u'Thu Sep 24 12:07:24 +0000 2015',
#  u'entities': {u'hashtags': [],
#   u'media': [{u'display_url': u'pic.twitter.com/MoQfYKFP1x',
#     u'expanded_url': u'http://twitter.com/valentinaluo/status/646849470425079808/photo/1',
#     u'id': 646849465689632768,
#     u'id_str': u'646849465689632768',
#     u'indices': [52, 74],
#     u'media_url': u'http://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#     u'media_url_https': u'https://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#     u'sizes': {u'large': {u'h': 579, u'resize': u'fit', u'w': 1024},
#      u'medium': {u'h': 339, u'resize': u'fit', u'w': 600},
#      u'small': {u'h': 192, u'resize': u'fit', u'w': 340},
#      u'thumb': {u'h': 150, u'resize': u'crop', u'w': 150}},
#     u'source_status_id': 646849470425079808,
#     u'source_status_id_str': u'646849470425079808',
#     u'source_user_id': 132734424,
#     u'source_user_id_str': u'132734424',
#     u'type': u'photo',
#     u'url': u'http://t.co/MoQfYKFP1x'}],
#   u'symbols': [],
#   u'urls': [],
#   u'user_mentions': [{u'id': 132734424,
#     u'id_str': u'132734424',
#     u'indices': [3, 16],
#     u'name': u'Valentina Luo',
#     u'screen_name': u'valentinaluo'}]},
#  u'extended_entities': {u'media': [{u'display_url': u'pic.twitter.com/MoQfYKFP1x',
#     u'expanded_url': u'http://twitter.com/valentinaluo/status/646849470425079808/photo/1',
#     u'id': 646849465689632768,
#     u'id_str': u'646849465689632768',
#     u'indices': [52, 74],
#     u'media_url': u'http://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#     u'media_url_https': u'https://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#     u'sizes': {u'large': {u'h': 579, u'resize': u'fit', u'w': 1024},
#      u'medium': {u'h': 339, u'resize': u'fit', u'w': 600},
#      u'small': {u'h': 192, u'resize': u'fit', u'w': 340},
#      u'thumb': {u'h': 150, u'resize': u'crop', u'w': 150}},
#     u'source_status_id': 646849470425079808,
#     u'source_status_id_str': u'646849470425079808',
#     u'source_user_id': 132734424,
#     u'source_user_id_str': u'132734424',
#     u'type': u'photo',
#     u'url': u'http://t.co/MoQfYKFP1x'}]},
#  u'favorite_count': 0,
#  u'favorited': False,
#  u'geo': None,
#  u'id': 647019500035002368,
#  u'id_str': u'647019500035002368',
#  u'in_reply_to_screen_name': None,
#  u'in_reply_to_status_id': None,
#  u'in_reply_to_status_id_str': None,
#  u'in_reply_to_user_id': None,
#  u'in_reply_to_user_id_str': None,
#  u'is_quote_status': False,
#  u'lang': u'en',
#  u'place': None,
#  u'possibly_sensitive': False,
#  u'retweet_count': 463,
#  u'retweeted': False,
#  u'retweeted_status': {u'contributors': None,
#   u'coordinates': None,
#   u'created_at': u'Thu Sep 24 00:51:46 +0000 2015',
#   u'entities': {u'hashtags': [],
#    u'media': [{u'display_url': u'pic.twitter.com/MoQfYKFP1x',
#      u'expanded_url': u'http://twitter.com/valentinaluo/status/646849470425079808/photo/1',
#      u'id': 646849465689632768,
#      u'id_str': u'646849465689632768',
#      u'indices': [34, 56],
#      u'media_url': u'http://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#      u'media_url_https': u'https://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#      u'sizes': {u'large': {u'h': 579, u'resize': u'fit', u'w': 1024},
#       u'medium': {u'h': 339, u'resize': u'fit', u'w': 600},
#       u'small': {u'h': 192, u'resize': u'fit', u'w': 340},
#       u'thumb': {u'h': 150, u'resize': u'crop', u'w': 150}},
#      u'type': u'photo',
#      u'url': u'http://t.co/MoQfYKFP1x'}],
#    u'symbols': [],
#    u'urls': [],
#    u'user_mentions': []},
#   u'extended_entities': {u'media': [{u'display_url': u'pic.twitter.com/MoQfYKFP1x',
#      u'expanded_url': u'http://twitter.com/valentinaluo/status/646849470425079808/photo/1',
#      u'id': 646849465689632768,
#      u'id_str': u'646849465689632768',
#      u'indices': [34, 56],
#      u'media_url': u'http://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#      u'media_url_https': u'https://pbs.twimg.com/media/CPoSKXGU8AA3gZW.jpg',
#      u'sizes': {u'large': {u'h': 579, u'resize': u'fit', u'w': 1024},
#       u'medium': {u'h': 339, u'resize': u'fit', u'w': 600},
#       u'small': {u'h': 192, u'resize': u'fit', u'w': 340},
#       u'thumb': {u'h': 150, u'resize': u'crop', u'w': 150}},
#      u'type': u'photo',
#      u'url': u'http://t.co/MoQfYKFP1x'}]},
#   u'favorite_count': 460,
#   u'favorited': False,
#   u'geo': None,
#   u'id': 646849470425079808,
#   u'id_str': u'646849470425079808',
#   u'in_reply_to_screen_name': None,
#   u'in_reply_to_status_id': None,
#   u'in_reply_to_status_id_str': None,
#   u'in_reply_to_user_id': None,
#   u'in_reply_to_user_id_str': None,
#   u'is_quote_status': False,
#   u'lang': u'en',
#   u'place': None,
#   u'possibly_sensitive': False,
#   u'retweet_count': 463,
#   u'retweeted': False,
#   u'source': u'<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>',
#   u'text': u"Ain't no sunshine when Xi's gone. http://t.co/MoQfYKFP1x",
#   u'truncated': False,
#   u'user': {u'contributors_enabled': False,
#    u'created_at': u'Wed Apr 14 01:42:16 +0000 2010',
#    u'default_profile': False,
#    u'default_profile_image': False,
#    u'description': u'Formerly at The Daily Telegraph & AFP, now @NOS, and occasional scribbler for Nikkei Asian Review (@NAR)',
#    u'entities': {u'description': {u'urls': []}},
#    u'favourites_count': 3490,
#    u'follow_request_sent': False,
#    u'followers_count': 1817,
#    u'following': False,
#    u'friends_count': 654,
#    u'geo_enabled': True,
#    u'has_extended_profile': False,
#    u'id': 132734424,
#    u'id_str': u'132734424',
#    u'is_translation_enabled': False,
#    u'is_translator': False,
#    u'lang': u'en',
#    u'listed_count': 129,
#    u'location': u'China',
#    u'name': u'Valentina Luo',
#    u'notifications': False,
#    u'profile_background_color': u'F5B1C8',
#    u'profile_background_image_url': u'http://pbs.twimg.com/profile_background_images/717346355/442b637517f47e3f083476101592c96d.jpeg',
#    u'profile_background_image_url_https': u'https://pbs.twimg.com/profile_background_images/717346355/442b637517f47e3f083476101592c96d.jpeg',
#    u'profile_background_tile': True,
#    u'profile_banner_url': u'https://pbs.twimg.com/profile_banners/132734424/1438911290',
#    u'profile_image_url': u'http://pbs.twimg.com/profile_images/657408131870425088/wSkcnzNj_normal.jpg',
#    u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/657408131870425088/wSkcnzNj_normal.jpg',
#    u'profile_link_color': u'DD2E44',
#    u'profile_sidebar_border_color': u'FFFFFF',
#    u'profile_sidebar_fill_color': u'DDEEF6',
#    u'profile_text_color': u'333333',
#    u'profile_use_background_image': True,
#    u'protected': False,
#    u'screen_name': u'valentinaluo',
#    u'statuses_count': 6239,
#    u'time_zone': u'Beijing',
#    u'url': None,
#    u'utc_offset': 28800,
#    u'verified': False}},
#  u'source': u'<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>',
#  u'text': u"RT @valentinaluo: Ain't no sunshine when Xi's gone. http://t.co/MoQfYKFP1x",
#  u'truncated': False,
#  u'user': {u'contributors_enabled': False,
#   u'created_at': u'Thu May 06 12:07:24 +0000 2010',
#   u'default_profile': True,
#   u'default_profile_image': False,
#   u'description': u"Macro Trader; tweeting on rates, FX, equities, commods, life's rich pageant...in roughly that order. Not looking to set the world to rights in 140 characters",
#   u'entities': {u'description': {u'urls': []}},
#   u'favourites_count': 2285,
#   u'follow_request_sent': False,
#   u'followers_count': 5549,
#   u'following': False,
#   u'friends_count': 1516,
#   u'geo_enabled': False,
#   u'has_extended_profile': False,
#   u'id': 140804291,
#   u'id_str': u'140804291',
#   u'is_translation_enabled': False,
#   u'is_translator': False,
#   u'lang': u'en',
#   u'listed_count': 219,
#   u'location': u'',
#   u'name': u'Five Minute Macro',
#   u'notifications': False,
#   u'profile_background_color': u'C0DEED',
#   u'profile_background_image_url': u'http://abs.twimg.com/images/themes/theme1/bg.png',
#   u'profile_background_image_url_https': u'https://abs.twimg.com/images/themes/theme1/bg.png',
#   u'profile_background_tile': False,
#   u'profile_image_url': u'http://pbs.twimg.com/profile_images/1418789805/leopard_normal.jpg',
#   u'profile_image_url_https': u'https://pbs.twimg.com/profile_images/1418789805/leopard_normal.jpg',
#   u'profile_link_color': u'0084B4',
#   u'profile_sidebar_border_color': u'C0DEED',
#   u'profile_sidebar_fill_color': u'DDEEF6',
#   u'profile_text_color': u'333333',
#   u'profile_use_background_image': True,
#   u'protected': False,
#   u'screen_name': u'5_min_macro',
#   u'statuses_count': 5267,
#   u'time_zone': u'London',
#   u'url': None,
#   u'utc_offset': 0,
#   u'verified': False}}
