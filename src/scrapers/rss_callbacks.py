import sys, datetime
from bson import json_util
import traceback
from Utilities import get_redis_connection0

redis_connection = get_redis_connection0()

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

def webapp_callback(body):
	pipe = redis_connection.pipeline(False)
	try:
		if isinstance(body, unicode) or isinstance(body, str):
			raw_message = json_util.loads(body)
			redis_connection.hmset(raw_message['id'], raw_message)
			pipe.expire(raw_message['id'], datetime.timedelta(days=30))
			pipe.sadd("WEBAPP_CACHE", raw_message['id'])
			pipe.expire("WEBAPP_CACHE", datetime.timedelta(days=30))
			pipe.execute()
	except Exception:
		raise Exception('Signal Detection Error -> %s' % traceback.format_exc())
	finally:
		sys.stderr.flush()
		sys.stdout.flush()

