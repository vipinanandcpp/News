import sys, argparse
from pandas_datareader import data, wb
import pandas as pd
import numpy as np
from scipy import stats
from Utilities import get_global_timezone, utc, eastern
import datetime
from db.newswires import get_news_data
from gensim.summarization import summarize
from gensim.summarization import keywords
from collections import Counter
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor

def arglist(astring):
	alist=astring.strip('[').strip(']').split(',')
	alist = [a.strip() for a in alist if len(a)]
	return alist

class model0:
	def __init__(self, frequencies, symbols, start_date, end_date, events_per_year, score_threshold):
		self.frequencies = frequencies
		self.symbols = symbols
		self.events_per_year = events_per_year
		self.model_name = 'model0'
		self.start_date = start_date
		self.end_date = end_date
		self.score_threshold = score_threshold

	def process_data(self):
		def _process(price_df, frequency, symbol):
			sys.stdout.write('Processing price data for %s frequency %d\n'%(symbol, frequency))
			price_df['return'] = np.log(price_df['Adj Close']) - np.log(price_df['Adj Close'].shift(frequency))
			price_df['buy'] = np.sign(price_df['return'])
			price_df['abs_return'] = np.abs(price_df['return'])
			price_df['score'] = price_df['abs_return'].apply(lambda x:stats.percentileofscore(price_df['abs_return'].dropna().values, x))
			total_events = self.events_per_year * max(len(price_df)/252,1)
			extreme_percentile = 1.0 - float(total_events)/float(len(price_df))
			_df = price_df[price_df['abs_return'] >= price_df['abs_return'].quantile(extreme_percentile)]
			_df = _df[_df['score'] >= self.score_threshold]
			return _df

		def gensim_summary_handler(article):
			ratio =1.0
			number_sentence = len(article.split('.'))
			if  number_sentence >= 5:
				ratio = 0.2
			else:
				ratio = ratio/number_sentence
			try:
				return summarize(article, ratio=ratio)
			except Exception as e:
				sys.stdout.write('gensim_summary_handler ERROR: %s\n'%(e.message))
				return ''

		def gensim_keywords_handler(article):
			ratio =1.0
			number_sentence = len(article.split('.'))
			if  number_sentence >= 5:
				ratio = 0.2
			else:
				ratio = ratio/number_sentence
			try:
				return keywords(article, ratio=ratio).split('\n')
			except Exception as e:
				sys.stdout.write('gensim_keywords_handler ERROR: %s\n'%(e.message))
				return []

		for symbol in self.symbols:
			price_df = data.DataReader(symbol, "yahoo", self.start_date, self.end_date)
			news_df = pd.DataFrame(get_news_data(start_date=self.start_date, end_date=self.end_date, tickers=[symbol]))
			with ThreadPoolExecutor(max_workers=len(self.frequencies)) as executor:
				futures = {executor.submit(_process, price_df.copy(), frequency, symbol):frequency for frequency in self.frequencies}
				result = {}
				for future in cf.as_completed(futures, timeout=600):
					frequency = futures[future]
					sys.stdout.write('Processing futures result for frequency %d of %s\n'%(frequency, symbol))
					r = future.result()
					date_range = [(date-datetime.timedelta(days=2), date+datetime.timedelta(days=2)) for date in r.index]
					filtered_df = pd.DataFrame()
					for lower_date, higher_date in date_range:
						filtered_df = filtered_df.append(news_df[(news_df['timestamp'] >= lower_date) & (news_df['timestamp'] <= higher_date)])
					filtered_df = filtered_df[filtered_df['article'].apply(lambda article: len(article) > 0)]
					filtered_df = filtered_df[filtered_df['tickers'].apply(lambda tickers: len(tickers) < 3)]
					filtered_df['gensim_keywords'] = filtered_df['article'].apply(lambda article:gensim_keywords_handler(article))
					filtered_df['gensim_summary'] = filtered_df['article'].apply(lambda article:gensim_summary_handler(article))
					filtered_df = filtered_df[filtered_df['gensim_keywords'].apply(lambda x:len(x) > 0)]
					filtered_df = filtered_df[filtered_df['gensim_summary'].apply(lambda x:len(x) > 0)]
					gensim_keywords_counter = Counter([keyword for _keywords in filtered_df['gensim_keywords'].values for keyword in _keywords])
					gensim_keywords_most_common = [word for word, count in gensim_keywords_counter.most_common(10) if len(word)]
					impact_news = filtered_df[filtered_df['gensim_keywords'].apply(lambda x: any([keyword in x for keyword in gensim_keywords_most_common]) == True)].T.to_dict().values()
					for news in impact_news:
						print news['_id'], news['timestamp'], news['title']
					del filtered_df
			del news_df

def runner(frequencies, symbols, events_per_year, start_date, end_date, score_threshold):
	model_instance = model0(frequencies, symbols, start_date, end_date, events_per_year, score_threshold)
	model_instance.process_data()

if __name__== '__main__':
	try:
		__IPYTHON__
		sys.stdout.write('\nrunning via ipython -> not running continously')
	except NameError:
		argparser = argparse.ArgumentParser(description='Impact model 0')
		argparser.add_argument('--frequencies', type=arglist, nargs='+')
		argparser.add_argument('--symbols', type=arglist, nargs='+')
		argparser.add_argument('--events_per_year', type=int, default=10)
		argparser.add_argument('--score', type=float, default=99.0)
		argparser.add_argument('dates', action="store",
						help="Date range to execute transfer. From-Thru: yyyy-mm-dd[:yyyy-mm-dd]",
						metavar="DATE", default=None)
		args = argparser.parse_args()
		dates = args.dates.split('=')[1] if args.dates and '=' in args.dates else None

		start_date = None
		end_date = None
		if dates is not None:
			date_list = dates.split(':')
			if len(date_list) > 1:
				start_date = date_list[0]
				end_date = date_list[-1]
			else:
				start_date = date_list[0]
				end_date = start_date
		else:
			start_date = (datetime.datetime.now() - datetime.timedelta(days=252)).date()
			end_date = datetime.datetime.now().date()
		frequencies = [int(freq) for freq in args.frequencies[0]]
		symbols = args.symbols[0]
		runner(frequencies, symbols, args.events_per_year, start_date, end_date, args.score)























