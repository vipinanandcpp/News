
# coding: utf-8

# In[1]:
from apscheduler.schedulers.blocking import BlockingScheduler
from pyquery import PyQuery
import pandas as pd

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# In[2]:
import time
import datetime
import win32com.client
import numpy as np
import collections
import pythoncom

import logging

sched = BlockingScheduler()
logging.basicConfig()


# In[3]:

import sys, logging, traceback
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor
from goose import Goose


headers = {
		'browser_user_agent': 'Mozilla'
	}

class URLProcessor(object):
	g = Goose()

	def __init__(self, max_workers=50):
		self.max_workers = max_workers

	@staticmethod
	def run_extraction(url):
		extracted = None
		try:
			extracted = URLProcessor.g.extract(url = url)
		except Exception as e:
			sys.stderr.write('\n\t' + str(traceback.print_exc()))
			logging.error(e, exc_info=1)
			extracted = None
		return extracted

	def process_url(self, url):
		return URLProcessor.run_extraction(url)

	def process_urls(self, urls, timeout = 600):
		results = {}
		with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
			futures = {executor.submit(self.process_url, url):url for url in urls}
			for future in cf.as_completed(futures, timeout=timeout):
				if future.result() is not None:
					url = futures[future]
					try:
						results[url] = future.result()
					except Exception as e:
						sys.stderr.write('\n\t' + str(traceback.print_exc()))
						logging.error(e, exc_info=1)
		return results
url_processor = URLProcessor()

def prepare_html(liston,color):
    liston_out=list()
    for headline,url in liston:
        if(len(headline)>0):
            out1_jose = "<tr><td BGCOLOR="+color+">" + headline + "<td align=center>" + url +  "</tr>";
            #out1_jose = "<tr><td BGCOLOR=#F7BE81>" + headline + "<td align=center>" + url +  "</tr>";
            liston_out.append(out1_jose)
    return liston_out
        
def prepare_wsj():
    pq = PyQuery('http://www.wsj.com/news/heard-on-the-street')

    base_url = 'http://www.wsj.com/'
    
    blog_url='http://blogs.wsj.com/'

    urls = []


    for item in pq('.headline-container .headline a').items():
        href  = item.attr['href']
        if base_url in href:
            urls.append(href)
        else:
            if blog_url not in href:
                urls.append(base_url+href)
            else:
                urls.append(href)

    #url_processor = URLProcessor()



    liston_wsj=list()


    for elem in urls:
        article=url_processor.process_url(elem)
        liston_wsj.append((article.title,elem))

    return liston_wsj    

def prepare_bb():
    pq = PyQuery('http://www.bloomberg.com')

    base_url = 'http://www.bloomberg.com/'

    urls = []


    for item in pq('.hero-v6-story__info-container .hero-v6-story__info a').items():
        href  = item.attr['href']
        if base_url in href:
            urls.append(href)
        else:
            urls.append(base_url+href)

    liston_bb=list()


    for elem in urls:
        article=url_processor.process_url(elem)
        liston_bb.append((article.title,elem))

    return liston_bb

def prepare_cnn():
    pq = PyQuery("http://money.cnn.com/news/economy/")


    #
    base_url = 'http://money.cnn.com/'


    urls = []


    for item in pq('article  a').items():
        href  = item.attr['href']
        if base_url in href:
            urls.append(href)
        else:
            urls.append(base_url+href)

    talla=len(urls)

    limite=min(talla,5)

    liston_cnn=list()



    for elem in urls[0:limite]:
        article=url_processor.process_url(elem)
        liston_cnn.append((article.title,elem))

    return liston_cnn


def prepare_bi():
    pq = PyQuery("http://www.businessinsider.com.au/category/10-things-before-opening-bell")


    #
    base_url = 'http://www.businessinsider.com.au/'


    urls = []


    for item in pq('.post-description .post-title a').items():
        href  = item.attr['href']
        if base_url in href:
            urls.append(href)
        else:
            urls.append(base_url+href)

    talla=len(urls)

    limite=min(talla,5)

    liston_bi=list()



    for elem in urls[0:limite]:
        article=url_processor.process_url(elem)
        liston_bi.append((article.title,elem))

    return liston_bi







# In[29]:

def prepare_big_csv(big_liston):
    path='enter your vipin path'
    today=datetime.datetime.now().date()
    df_out=pd.DataFrame(columns=['text','url'])
    contador=0
    for agency in big_liston:
        for news in agency:
            df_out.loc[contador]=news
            contador=contador+1
            
    df_out.to_csv(path+"\\"+"TOP_NEWS_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')
    print "news done"
           
            
            
        


def prepare_big_html(big_liston,color):
    total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u>  THINGS TO KNOW BEFORE THE BELL  </u></b></CAPTION>";
    total = total + "<tr><td><b>" + "TITLE" + "</b><td width='230' align=center><b>"  + "URL" + "</b></tr>";
    tempo=prepare_html(big_liston[0],color[0])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem
    tempo=prepare_html(big_liston[1],color[1])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem
    tempo=prepare_html(big_liston[2],color[2])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem

    tempo=prepare_html(big_liston[3],color[3])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem



    total = total + "</table></body></html>";

    return total
        
def send_email_new(users,total):
	today=datetime.datetime.now()
	text="TOP NEWS PREMARKET AT "+today.strftime('%m/%d/%Y %H:%M')
	s = smtplib.SMTP("smtp.jose.corp")
	me = 'jperez@me.com'
	you =users
	msg = MIMEMultipart('alternative')
	msg['Subject'] =text
	msg['From'] = me
	#msg['To'] =you
	msg['Bcc'] =you
	
	#html=total
	part1 = MIMEText(text, 'plain')
	part2 = MIMEText(total, 'html','utf-8')

	# Attach parts into message container.
	# According to RFC 2046, the last part of a multipart message, in this case
	# the HTML message, is best and preferred.
	msg.attach(part1)
	msg.attach(part2)
	s.sendmail(me, you.split(";"), msg.as_string().encode('ascii'))
	s.quit()

	
	
	

   
	print('email sent.')

	
	


def send_email(users,total):
    olMailItem = 0x0

    pythoncom.CoInitialize()
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = "TOP NEWS PREMARKET AT "+today.strftime('%m/%d/%Y %H:%M')
    #newMail.Body = "I I AM IN THE BODY\nSO AM I!!!"
    #newMail.Body="The purpose of this table is to provide a guide that I can generate at any time during the day for the most impoortant news that move the ticker. I would say that the table will be complete: all main events from the day will be there so you do not have to spend time trying to find an explanation somehwere else."
   
    newMail.HTMLBody=total
   
    #newMail.To = "who_to_send_to@example.com"
    newMail.CC="jose.pereza@me.com"
    #newMail.CC = "moreaddresses here"
    #newMail.BCC = "address"
    newMail.BCC=users
    #attachment1 = "Path to attachment no. 1"
    #attachment2 = "Path to attachment no. 2"
    #newMail.Attachments.Add(attachment1)
    #newMail.Attachments.Add(attachment2)
    #newMail.display()
    newMail.Send()
    print('email sent.')


@sched.scheduled_job('cron', day_of_week='mon-fri', hour=8,minute=45,misfire_grace_time=60)    
def timed_job():
    print('This job runs daily in the mornings, speedy news outlook.')
    pythoncom.CoInitialize()
  
    

    liston_bi=prepare_bi()
    liston_wsj=prepare_wsj()
    liston_bb=prepare_bb()
    liston_cnn=prepare_cnn()

    big_liston=[liston_bi,liston_cnn,liston_bb,liston_wsj]
    color=['#ff9900','#00FF00','#00FFFF','#F3F781']

    total=prepare_big_html(big_liston,color)

    

    users="jose@me.com"
    #users="jperez@joseatm.com;KAzmoon@joseatm.com"
    send_email_new(users,total)
    prepare_big_csv(big_liston)

@sched.scheduled_job('cron', day_of_week='sat-sun', hour=9,minute=30,misfire_grace_time=60)    
def timed_job():
    print('This job runs daily in the mornings, speedy news outlook.')
    pythoncom.CoInitialize()
  
    liston_bi=prepare_bi()

    liston_wsj=prepare_wsj()
    liston_bb=prepare_bb()
    liston_cnn=prepare_cnn()


    big_liston=[liston_bi,liston_cnn,liston_bb,liston_wsj]
    color=['#ff9900','#00FF00','#00FFFF','#F3F781']

    total=prepare_big_html(big_liston,color)

    

    users="jperez@josea.com"
    send_email_new(users,total)
    prepare_big_csv(big_liston)
  
    





sched.start()

# In[ ]:



