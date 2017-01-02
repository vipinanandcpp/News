import sys
from bson import json_util
import traceback

def rss_callback0(body):
	try:
		if isinstance(body, unicode) or isinstance(body, str):
			raw_messages = json_util.loads(body)
			print raw_messages
	except Exception:
		raise Exception('Signal Detection Error -> %s' % traceback.format_exc())
	finally:
		sys.stderr.flush()
		sys.stdout.flush()

