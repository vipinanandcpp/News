import os
from data_processing_pipeline.twokenize import tokenizeRawTweetText as twokenize

working_directory = os.path.abspath(os.path.join(os.path.dirname(__file__)))

def clean_word(word):
	if '#' in word:
		word = word.split('#')[0]
	return word.strip().lower()

with open( os.path.join(working_directory, 'positive_words.txt') ) as f:
	positive_words = set( clean_word(word) for word in f.readlines())

with open( os.path.join(working_directory, 'negative_words.txt') ) as f:
	negative_words = set( clean_word(word) for word in f.readlines())

with open( os.path.join(working_directory, 'english.stop.txt') ) as f:
	stop_list = [word.strip() for word in f.readlines()]

def calculate_score(text):
	tokens = set(token.strip().lower() for token in twokenize(text) if '$' not in token)
	positive_tokens = list(positive_words.intersection(tokens))
	negative_tokens = list(negative_words.intersection(tokens))
	return (positive_tokens, negative_tokens)

