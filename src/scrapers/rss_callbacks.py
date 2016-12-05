import sys, kombu
from bson import json_util
from Utilities import rabbitmq_connection0
import traceback

def rss_callback0(body, message):
	outgoing_exchange = kombu.Exchange(name="rss_email_exchange", type='direct')
	outgoing_producer = kombu.Producer(rabbitmq_connection0, exchange=outgoing_exchange, routing_key = 'rss', serializer='json', compression=None, auto_declare=True)
	if rabbitmq_connection0.connected:
		outgoing_exchange(rabbitmq_connection0).declare()
		try:
			if isinstance(body, unicode) or isinstance(body, str):
				raw_messages = json_util.loads(body)
				print raw_messages
				if len(raw_messages):
					outgoing_producer.publish(json_util.dumps(raw_messages), serializer='json')
		except Exception:
			raise Exception('Signal Detection Error -> %s' % traceback.format_exc())
		finally:
			message.ack()
			sys.stderr.flush()
			sys.stdout.flush()

