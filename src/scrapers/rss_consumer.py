import sys, traceback, argparse, threading, signal, atexit
from pika_consumer import PIKA_CONSUMER
import rss_callbacks
from Utilities import hostname0

def arglist(astring):
	alist=astring.strip('[').strip(']').split(',')
	alist = [getattr(rss_callbacks, a.strip()) for a in alist if len(a)]
	return alist[0] if len(alist) else []

class RSS_CONSUMER(object):
	def __init__(self, routing_key, incoming_exchange_name, queue_name, callbacks = []):
		self.routing_key = routing_key
		self.incoming_exchange_name = incoming_exchange_name
		self.queue_name = queue_name
		self.callbacks = callbacks
		self.pika_consumer = PIKA_CONSUMER('amqp://%s'%(hostname0), routing_key = self.routing_key, exchange_name = self.incoming_exchange_name, queue_name = self.queue_name,  exchange_type = 'direct', callbacks = self.callbacks)
		for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
			signal.signal(sig, self.signal_handler)
		atexit.register(self.signal_handler, self)

	def __del__(self):
		sys.stdout.write("Shutting down RSS consumer channel\n")
		self.pika_consumer.stop()

	def start_consumer(self):
		sys.stdout.write('Starting consumer for %s %s %s\n'%(self.incoming_exchange_name, self.routing_key, self.queue_name))
		threading.Thread(target = self.pika_consumer.run).start()
		while True:
			pass

	def signal_handler(self, signum = None, frame = None):
		self.__del__()
		sys.exit(0)


def main(args):
	routing_key = args.routing_key
	exchange_name = args.exchange
	queue_name = args.queue_name
	callbacks = args.callbacks
	rss_consumer = RSS_CONSUMER(routing_key, exchange_name, queue_name, callbacks)
	rss_consumer.start_consumer()

if __name__ == '__main__':
	try:
		__IPYTHON__
	except NameError:
		argparser = argparse.ArgumentParser(description='RSS alerts consumer')
		argparser.add_argument('-e', '--incoming_exchange_name', action='store', dest='exchange', help='exchange', required=True)
		argparser.add_argument('-r', '--incoming_routing_key', action='store', dest='routing_key', help='routing_key', required=True)
		argparser.add_argument('-q', '--queue_name', action='store', dest='queue_name', help='queue_name', required=True)
		argparser.add_argument('--callbacks', type=arglist, nargs='+')
		args = argparser.parse_args()
		main(args)
