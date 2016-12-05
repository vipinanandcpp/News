import sys, kombu, traceback, argparse
import rss_callbacks
from Utilities import rabbitmq_connection0

def arglist(astring):
	alist=astring.strip('[').strip(']').split(',')
	alist = [getattr(rss_callbacks, a.strip()) for a in alist if len(a)]
	return alist[0] if len(alist) else []

class RSS_CONSUMER(object):
	def __init__(self, routing_key, incoming_exchange_name, callbacks = []):
		self.routing_key = routing_key
		self.incoming_exchange_name = incoming_exchange_name
		self.callbacks = callbacks

	def start_consumer(self):
		queues = []
		incoming_exchange = kombu.Exchange(name=self.incoming_exchange_name, type='direct')
		incoming_queue = kombu.Queue(name=self.routing_key+'_'+self.incoming_exchange_name, exchange=incoming_exchange, routing_key=self.routing_key)
		queues.append(incoming_queue)
		if rabbitmq_connection0.connected:
			incoming_exchange(rabbitmq_connection0).declare()
			incoming_queue(rabbitmq_connection0).declare()
			with rabbitmq_connection0.Consumer(queues=queues, callbacks=self.callbacks) as consumer:
				sys.stdout.write('\n Starting consumer for %s %s\n'%(self.routing_key, self.incoming_exchange_name))
				while True:
					rabbitmq_connection0.drain_events()

def main(args):
	routing_key = args.incoming_routing_key
	exchange_name = args.incoming_exchange_name
	callbacks = args.callbacks
	rss_consumer = RSS_CONSUMER(routing_key, exchange_name, callbacks)
	rss_consumer.start_consumer()

if __name__ == '__main__':
	try:
		__IPYTHON__
	except NameError:
		argparser = argparse.ArgumentParser(description='RSS alerts consumer')
		argparser.add_argument('-e', '--incoming_exchange_name', action='store', dest='exchange', help='exchange', required=True)
		argparser.add_argument('-r', '--incoming_routing_key', action='store', dest='routing_key', help='routing_key', required=True)
		argparser.add_argument('--callbacks', type=arglist, nargs='+')
		args = argparser.parse_args()
		main(args)
