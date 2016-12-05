import sys, time
from apscheduler.schedulers.background import BackgroundScheduler


class Scheduler(object):
	def __init__(self, trigger = 'cron', day_of_week=None, hour=None,minute=None, seconds= None, misfire_grace_time=60, args = [], callbacks = []):
		self.trigger = trigger
		self.day_of_week = '*' if day_of_week is None else day_of_week
		self.hour = 0 if hour is None else hour
		self.minute = 0 if minute is None else minute
		self.seconds = 0 if seconds is None else seconds
		self.misfire_grace_time = misfire_grace_time
		self.args = args
		self.callbacks = callbacks

	def __str__(self):
		return self.day_of_week+"_"+self.trigger+"_"+self.hour+"_"+self.minute+"_"+self.seconds

	def __call__(self):
		scheduler = BackgroundScheduler()
		for idx, callback in enumerate(self.callbacks):
			scheduler.add_job(callback, self.trigger, id=self+"_"+str(idx), minute=self.minute, hour=self.hour, seconds=self.seconds, misfire_grace_time=self.misfire_grace_time, args=self.args)
		scheduler.start()
		sys.stdout.write('Starting %s schdeuler\n'%(self))
		try:
			while True:
				time.sleep(2)
		except (KeyboardInterrupt, SystemExit):
			scheduler.shutdown()
		finally:
			sys.stdout.flush()
