import os, gzip, datetime, sys, requests
from pyquery import PyQuery
import pandas as pd

try:
# For Python 3.0 and later
	from urllib import urlopen
except ImportError:
# Fall back to Python 2's urllib2
	from urllib2 import urlopen

def get_tickers_by_industry_exchange_nasdaq():
	exchanges=['NASDAQ', 'NYSE', 'AMEX']
	base_url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?'
	tickers_df = None
	for exchange in exchanges:
		if exchange == 'NASDAQ':
			markets = ['NASDAQ', 'NGS', 'NGM', 'NCM', 'ADR']
			for market in markets:
				source_url = base_url+'exchange={exchange}&market={market}&render=download'.format(exchange=exchange, market=market)
				sys.stdout.write('Reading %s\n'%(source_url))
				data = urlopen(source_url).readlines()
				if not len(data):
					sys.stderr.write('No data found for %s\n'%(source_url))
					continue
				columns = [d.replace('\"', "") for d in data[0].replace(',\r\n','').split("\",")]
				clean_data = []
				for d in data[1:]:
					clean_data.append([_d.replace('\"', "").strip() for _d in d.replace(',\r\n','').split("\",")])
				df = pd.DataFrame(columns=columns, data=clean_data)
				df['exchange'] = exchange
				if tickers_df is None:
					tickers_df = df
				else:
					tickers_df = tickers_df.append(df)
				del clean_data, data
		else:
			source_url = base_url+'exchange={exchange}&render=download'.format(exchange=exchange)
			sys.stdout.write('Reading %s\n'%(source_url))
			data = urlopen(source_url).readlines()
			if not len(data):
				sys.stderr.write('No data found for %s\n'%(source_url))
				continue
			columns = [d.replace('\"', "") for d in data[0].replace(',\r\n','').split("\",")]
			clean_data = []
			for d in data[1:]:
				clean_data.append([_d.replace('\"', "").strip() for _d in d.replace(',\r\n','').split("\",")])
			df = pd.DataFrame(columns=columns, data=clean_data)
			df['exchange'] = exchange
			if tickers_df is None:
				tickers_df = df
			else:
				tickers_df = tickers_df.append(df)
			del clean_data, data
	filtered_df = tickers_df[['Symbol', 'Name', 'Sector', 'Industry', 'exchange']]
	filtered_df.set_index('Symbol', drop=True, inplace=True)
	return filtered_df.T.to_dict()


def get_etfs_data():
	base_url = 'http://www.nasdaq.com/investing/etfs/etf-finder-results.aspx'
	r = requests.get(base_url)
	pq = PyQuery(r.content)
	sectors = ['-'.join(d[0].lower().split()) for d in zip(*[iter(pq('#two_column_main_content_DropDownsector option').contents())])][1:]
	etfs_db = None
	for sector in sectors:
		source_url = base_url+'?sector=%s&download=Yes'%(sector)
		sys.stdout.write('Reading %s\n'%(source_url))
		df = pd.read_csv(source_url)
		df['Sector'] = sector
		df['Industry'] = 'Finance'
		df['exchange'] = None
		if etfs_db is None:
			etfs_db = df
		else:
			etfs_db = etfs_db.append(df)
	filtered_df = etfs_db[['Symbol', 'Name', 'Sector', 'Industry', 'exchange']]
	filtered_df.set_index('Symbol', drop=True, inplace=True)
	return  filtered_df.T.to_dict()

def get_tickers_mapping():
	tickers = {}
	tickers.update(get_tickers_by_industry_exchange_nasdaq())
	tickers.update(get_etfs_data())
	return tickers


