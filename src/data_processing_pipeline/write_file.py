import os, sys, datetime, time, gzip, signal
from bson import json_util
import settings
import atexit
from Utilities import utc, eastern

try:
	#python 3 compatibility
	from io import TextIOWrapper as file
except NameError:
	pass

class WriteFile(object):
	def __init__(self, group, filename):
		self.group = group
		self.filename = filename

		self.f = None
		self.open_file()
		self.d_str = utc.localize(datetime.datetime.utcnow()).astimezone(eastern).strftime('%Y%m%d')

		for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
			signal.signal(sig, self.signal_handler)
		atexit.register(self.signal_handler, self)

	def __del__(self):
		if self.f.closed is False:
			self.close_file()

	def signal_handler(self, signum = None, frame = None):
		# sys.stdout.write('\nSignal handler called with signal', signum)
		self.close_file()
		time.sleep(5)	#here check if process is done
		sys.stdout.write('\tWait done!')
		sys.exit(0)

	def open_file(self):
		self.close_file()
		d = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)
		self.working_directory = os.path.join(settings.data_directory, 'daily', d.strftime('%Y%m%d'), self.group)
		if not os.path.exists( self.working_directory ):
			os.makedirs( self.working_directory )

		self.filename_suffix = 0
		while(os.path.exists( os.path.join(self.working_directory, self.filename + "_" +  str(self.filename_suffix) + '_json.txt.gz') )):
			self.filename_suffix += 1

		if int(settings.python_main_version) < 3:
			self.f = gzip.GzipFile( os.path.join( self.working_directory, self.filename  + "_" +  str(self.filename_suffix) + '_json.txt.gz' ), mode='at', compresslevel=9)
		else:
			self.f = gzip.open( os.path.join( self.working_directory, self.filename  + "_" +  str(self.filename_suffix) + '_json.txt.gz' ), mode='at', compresslevel=9)
		sys.stdout.write('\nsuccessfullly opened %s' % self.f.filename if int(settings.python_main_version) < 3 else self.f.name)

	def close_file(self):
		if isinstance(self.f, file) or isinstance(self.f, gzip.GzipFile):
			if self.f.closed is False:
				self.f.flush()
			self.f.close()
			sys.stdout.write('\nsuccessfully closed %s' % self.f.filename if int(settings.python_main_version) < 3 else self.f.name)

	def process_message(self, message):
		today = utc.localize(datetime.datetime.utcnow()).astimezone(eastern).strftime('%Y%m%d')

		if self.f is None or self.d_str != today or self.f.closed:
			self.d_str = today
			self.open_file()

		if self.f:
			self.f.write(json_util.dumps(message) + '\n')
			self.f.flush()

	def process_messages(self, messages):
		for message in messages:
			self.process_message(message)

def test():
	w = WriteFile(group='testing', filename='test1')
	messages = ['test1', 'test2', 'test3']
	w.process_messages(messages)
	w.close_file()
	w.process_messages(messages)
	w.close_file()
	del w

