import datetime, sys, re, socket
from bson import json_util
from dateutil import parser
import logging
import pandas as pd
import pytz
import settings
import os
import pymongo, redis, pika
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures as cf
#import MySQLdb

try:
	import configparser as ConfigParser
except ImportError:
	import ConfigParser

eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')
gmt = pytz.timezone('GMT')
central_US_timezone = pytz.timezone('US/Central')

python_version = sys.version_info.major
pika_version = pika.__version__

ticker_entity_mapping = {}
for p in pd.read_csv(os.path.join(settings.src_files, 'data_files', 'ticker_entity_mapping.csv'))[['Symbol', 'Name']].itertuples(index=False):
	ticker_entity_mapping.setdefault(p.Name, p.Symbol)

with open(os.path.join(settings.src_files, "config", "servers.json")) as f:
	servers = json_util.loads(f.read())

def get_hostname(url='gmail.com'):
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	try:
		s.connect( (url,80) )
		hostname = (s.getsockname()[0])
	finally:
		s.close()
	return hostname

def identify_server(current_public_ip):
	instance_id = None
	hostname0 = None
	for server in servers:
		if servers[server]['public_ip'] == current_public_ip:
			instance_id = servers[server]["ID"]
			hostname0 = servers["processing"]['private_ip']
			break
	if instance_id is None:
		instance_id = 'offline'
		hostname0 = servers['processing']['public_ip']
	return instance_id, hostname0

current_public_ip = get_hostname()
instance_id, hostname0 = identify_server(current_public_ip)

# news data
def get_redis_connection0():
	if current_public_ip == hostname0:
		return redis.StrictRedis(unix_socket_path='/tmp/redis9.sock', host=hostname0, port=6379, db=0)
	else:
		return redis.StrictRedis(host=hostname0, port=6379, db=0)

def loadConfigFile(configFile):
	if configFile is None:
		return configFile

	config = ConfigParser.SafeConfigParser()
	config.readfp(configFile)
	return config

def getConfiguration(configurationFilePath):
	if configurationFilePath is None:
		return configurationFilePath
	configFile = open(configurationFilePath, 'r')
	configuration = loadConfigFile(configFile)
	configFile.close()
	return configuration

def getConfigSectionAsMap(config, sectionID):
	valueMap = {}
	if config is not None and config.has_section(sectionID):
		valueDict = config._sections.get(sectionID)
		valueMap = {key : value for (key, value) in valueDict.items() if key !=  '__name__'}
	else:
		logging.error("No %s section in config"%sectionID)

	return valueMap

def createMySQLConnection(databaseInfo):
	return MySQLdb.connect(host = databaseInfo.host,
						   user = databaseInfo.user,
						   passwd = databaseInfo.password,
						   db = databaseInfo.databaseName)

def createMongoDBConnection(databaseInfo):
	return pymongo.MongoClient(databaseInfo.host, tz_aware=True, connect=True)


def getMySQLDatabaseInfo(infoMap):
	return DatabaseInfo(infoMap.get('host', "None"), infoMap.get('user', "None"),
								  infoMap.get('password', ""), infoMap.get('database', "None"), infoMap.get('port', 3306))

def getMongoDatabaseInfo(infoMap):
	return DatabaseInfo(infoMap.get('host', "None"), infoMap.get('user', "None"),
								  infoMap.get('password', ""), infoMap.get('database', "None"), infoMap.get('port', 27017))

def getRedisDatabaseInfo(infoMap):
	return DatabaseInfo(infoMap.get('host', "None"), infoMap.get('user', "None"),
								  infoMap.get('password', ""), infoMap.get('database', "None"), infoMap.get('port', 6379))

def openMongoDBConnection(configurationFilePath, sectionID):
	config = getConfiguration(configurationFilePath)
	infoMap = getConfigSectionAsMap(config, sectionID)
	databaseInfo = getMongoDatabaseInfo(infoMap)
	return createMongoDBConnection(databaseInfo)

def openMySQLConnection(configurationFilePath, sectionID):
	config = getConfiguration(configurationFilePath)
	infoMap = getConfigSectionAsMap(config, sectionID)
	databaseInfo = getMySQLDatabaseInfo(infoMap)
	return createMySQLConnection(databaseInfo)

def listChunkIterator(myList, chunkSize):
	for idx in range(0, len(myList), chunkSize):
		yield myList[idx:(idx+chunkSize)]

def get_global_timezone(_datetime, local_tzinfo = eastern):
	timestamp = None
	if _datetime is not None:
		if isinstance(_datetime, datetime.datetime) or isinstance(_datetime, datetime.date):
			timestamp = _datetime
		else:
			try:
				timestamp = parser.parse(_datetime)
			except Exception as e:
				timestamp = pd.to_datetime(_datetime)
		if timestamp.tzinfo is None:
			timestamp = local_tzinfo.localize(timestamp)
		timestamp = timestamp.astimezone(utc)
	return timestamp

def find_tickers(text):
	exchanges = ('NYSE', 'NASDAQ', 'AMEX', 'OTC', 'TSX')
	tickers = []
	_text = text
	text = re.sub('\n| ', '', text.upper())
	text = re.sub('\[', '\(', text)
	text = re.sub('\]', '\)', text)
	for exchange in exchanges:
		tokens = text.split('(' + exchange)
		if len(tokens)==1:
			continue
		for match in tokens[1:]:
			_match = ''
			try:
				_match = match.split(':')
			except IndexError:
				break
			if len(_match):
				ticker = ''
				for char in _match:
					if char.isalpha():
						ticker += char
					else:
						break
				tickers.append(ticker)

	def f(names, txt):
		_tickers = []
		for name in names:
			if isinstance(name, str):
				if name in txt:
					_ticker = ticker_entity_mapping[name]
					if _ticker not in tickers:
						_tickers.append(_ticker)
		return _tickers

	with ThreadPoolExecutor(max_workers=1000) as executor:
		futures = {executor.submit(f, chunk, _text):chunk_number for chunk_number, chunk in enumerate(listChunkIterator(ticker_entity_mapping.keys() if python_version <= 2 else list(ticker_entity_mapping.keys()), 1000))}
		for future in cf.as_completed(futures, timeout=600):
			result = future.result()
			if len(result):
				tickers.extend(result)
	return [_ticker for _ticker in tickers if len(_ticker)]

class DatabaseInfo:
	def __init__(self, databaseHost, databaseUser, databasePWD, databaseName, serverPort, sid=None):
		self.host = databaseHost
		self.user = databaseUser
		self.password = databasePWD
		self.databaseName = databaseName
		self.port = int(serverPort)
		self.sid = sid
