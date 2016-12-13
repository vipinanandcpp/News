import re
from sklearn.feature_extraction.text import CountVectorizer
from data_processing_pipeline.preprocess import process_message, preprocess_text
from data_processing_pipeline.preprocess import tokenize_nopunct
from data_processing_pipeline.preprocess import clean_tokens_nopunct_nostopwords
from data_processing_pipeline.preprocess import tokenize
from data_processing_pipeline.preprocess import snowballize
from data_processing_pipeline.preprocess import tokenize_max
from data_processing_pipeline.preprocess import superprocess_text

strip_url = lambda url: re.sub('((www\.[\s]+)|(https?://[^\s]+))', "", url)

#from data_processing_pipeline.preprocess import prepare_text, tokenize
#from data_processing_pipeline.stoplists import stop

clean_text = lambda text: process_message(text.lower(), '', '', '', '')
clean_text_preserve = lambda text: process_message(text.lower(), '', '', '', '', remove_rt_marker=True, check_numbers=False)

tweet_vectorizer = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize, preprocessor=clean_text).build_analyzer()

tweet_vectorizer_preserve = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize, preprocessor=clean_text_preserve).build_analyzer()

tweet_snowball_vectorizer = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=snowballize, preprocessor=clean_text).build_analyzer()

ngram_vectorizer = lambda n: CountVectorizer(stop_words=None, strip_accents='unicode', decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(1,n), lowercase=True).build_analyzer()

twitter_ngram_splitter = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize_nopunct, lowercase=True, preprocessor=preprocess_text).build_analyzer()

demo_splitter = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize_max, lowercase=True).build_analyzer()

nopunct_splitter = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize_nopunct, lowercase=True).build_analyzer()

combined_splitter = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=tokenize_max, lowercase=True, preprocessor=superprocess_text).build_analyzer()

clean_twitter_ngram_splitter = lambda a,b: CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), tokenizer=clean_tokens_nopunct_nostopwords, lowercase=True, preprocessor=superprocess_text).build_analyzer()

# http://stackoverflow.com/questions/20717641/countvectorizer-i-not-showing-up-in-vectorized-text
twitter_ngram_splitter_case_sensitive = lambda a,b:  CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), token_pattern='(?u)\\b\\w+\\b', lowercase=False, preprocessor=strip_url).build_analyzer()

twitter_ngram_splitter_case_sensitive_apostrophe_s = lambda a,b:  CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), token_pattern="\\b\\w+\\'?s?\\b", lowercase=False, preprocessor=strip_url).build_analyzer()

twitter_ngram_splitter_case_insensitive = lambda a,b:  CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), token_pattern='(?u)\\b\\w+\\b', lowercase=True, preprocessor=strip_url).build_analyzer()
