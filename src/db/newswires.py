import os, sys, datetime, traceback
from dateutil import parser
import settings
from Utilities import openMongoDBConnection, find_tickers, get_global_timezone, utc
from bson.objectid import ObjectId

mongodb_connection = openMongoDBConnection(os.path.join(settings.database_directory, 'database.config'), 'mongoDB')
date_format = "%a %b %d %H:%M:%S %Y"


def get_news_data(start_date = None, end_date = None, tickers = [], sources = [], default_lookback = 365.25):
	if start_date is None:
		start_date = datetime.datetime.utcnow() - datetime.timedelta(days=default_lookback)
	elif isinstance(start_date, str):
		start_date = parser.parse(start_date)
	assert isinstance(start_date, datetime.datetime)

	if end_date is None:
		end_date = datetime.datetime.utcnow()
	elif isinstance(end_date, str):
		end_date = parser.parse(end_date)
	assert isinstance(end_date, datetime.datetime)

	query = {}

	query['timestamp'] = {'$gte': start_date, '$lte':end_date}

	if len(tickers) > 0:
		if len(tickers) == 1:
			query['tickers'] = tickers[0]
		else:
			query['tickers'] = {'$in':tickers}

	if len(sources) > 0:
		if len(sources) == 1:
			query['source'] = sources[0]
		else:
			query['source'] = {'$in':sources}

	db = mongodb_connection['news']
	collections = db.collection_names()
	for collection in collections:
		for data in mongodb_connection['news'][collection].find(query):
			yield data


def _update_tickers(): #one time update done on missing tickers never to be utilized ever again!!!
	db = mongodb_connection['news']
	collections = db.collection_names()
	for collection in collections:
		for data in mongodb_connection['news'][collection].find():
			old_tickers = data['tickers']
			tickers = find_tickers(data['article'])
			_id = ObjectId(data['_id'])
			if len(tickers):
				new_tickers = [ticker for ticker in list(set(old_tickers).union(set(tickers))) if len(ticker.strip())]
				mongodb_connection['news'][collection].update_one({'_id' : _id}, {'$set' : {'tickers':new_tickers, 'lud':get_global_timezone(datetime.datetime.utcnow(), local_tzinfo = utc)} }, upsert=False)
				mongodb_connection['news'][collection].create_index('tickers', background = True)
				sys.stdout.write('Tickers updated for %s\n'%(_id))



