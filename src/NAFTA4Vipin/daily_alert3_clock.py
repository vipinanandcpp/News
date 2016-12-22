
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
#import win32com.client
import numpy as np
import collections
#import pythoncom
import feedparser
import rfc822
import email

import logging

sched = BlockingScheduler()
logging.basicConfig()


# In[3]:

import sys, traceback

# In[2]:

import logging, settings
from url_processor import URLProcessor

from datetime import  timedelta
from os import path
import jsonpickle

import os, pytz
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')


# In[3]:

url_processor = URLProcessor()
palabras=['leaving country','leaving the country','leave country','leave the country','flee','fleeing','expensive','tax','taxes','factory','the border','manufacturing','remittance','carrier','indiana','indianapolis','plant','trade','tariff','tariffs','wall','mexican','mexicans','mexico','kentucky','ford','general electric','deport','deportation','deporting','immigrant','immigrants','immigration','illegal','cartels','enrique','nieto','vicente']

# In[4]:

def prepare_html(liston,color):
    liston_out=list()
    for headline,url in liston:
        if(len(headline)>0):
            out1_jose = "<tr><td BGCOLOR="+color+">" + headline + "<td align=center>" + url +  "</tr>";
            #out1_jose = "<tr><td BGCOLOR=#F7BE81>" + headline + "<td align=center>" + url +  "</tr>";
            liston_out.append(out1_jose)
    return liston_out


# In[5]:

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





# In[6]:

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


# In[7]:

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


# In[8]:

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

    limite=min(talla,2)

    liston_bi=list()



    for elem in urls[0:limite]:
        article=url_processor.process_url(elem)
        liston_bi.append((article.title,elem))

    return liston_bi


# In[9]:

def prepare_financiero():
    urls=[]
    texto=[]
    base_url = 'http://www.elfinanciero.com.mx'
    pq = PyQuery('http://www.elfinanciero.com.mx/economia/')

    for item in pq('.category-main-news .title a'):
        #print item.text
        href  = item.attrib['href']
        if base_url in href:
            urls.append(href)
            texto.append(item.text)
        else:
            urls.append(base_url+href)
            texto.append(item.text)
    liston_financiero=list()

    for aux  in range(len(urls)):
        liston_financiero.append((texto[aux],urls[aux]))
    return  liston_financiero

def prepare_economista():


    base_url ='http://eleconomista.com.mx/'

    pq = PyQuery("http://eleconomista.com.mx/")


    days_2_past=0
    today=datetime.datetime.now() - timedelta(days=days_2_past)
    today=today.date()
    today_str=today.strftime("%Y/%m/%d")
    url_text={}
    #for item in pq('.category-main-news .title a'):
    for item in pq('.view-content .field-content a').items():
        #print item.text
        #href  = item.attrib['href']
        href  = item.attr['href']
        if today_str not in href:
            continue
        if base_url in href:
            #urls.append(href)
            url_text[href]=item.text()
            #texto.append(item.text)
        else:
            #urls.append(base_url+href)
            url_text[base_url+href]=item.text()
            #texto.append(item.text)


    liston_economista=list()

    for key in url_text.keys():
        liston_economista.append((url_text[key],key))
    return  liston_economista

def url_date(url):
    d = feedparser.parse(url)
    lista=[]
    ff=d['entries']
    for elem in ff:
        try:
            fecha=elem['published']
            pp=rfc822_f(fecha)
            gg=utc.localize(pp).astimezone(eastern)
            today=datetime.datetime.now()
            if gg.date()>=today.date():

                #print elem['links'][0]['href'],elem['published'],elem.title_detail.value
                lista.append((elem.title_detail.value,elem['links'][0]['href']))

        except Exception as e:
            sys.stderr.write('\n\t' + str(traceback.print_exc()))
            logging.error(e, exc_info=1)

    return lista

def rfc822_f(timestamp):
    return datetime.datetime.fromtimestamp(email.Utils.mktime_tz(email.Utils.parsedate_tz(timestamp)))
def prepare_rss_mxn():
    jornada="http://www.jornada.unam.mx/rss/economia.xml?v=1"
    universal="http://www.eluniversal.com.mx/seccion/14/rss.xml"
    economista="http://eleconomista.com.mx/ultimas-noticias/rss"
    economista1="http://eleconomista.com.mx/dinero/rss"
    economista2="http://eleconomista.com.mx/finanzas-personales/rss"
    economista3="http://eleconomista.com.mx/mercados/rss"
    economista4="http://eleconomista.com.mx/empresas/rss"

    galaxia=[jornada, universal, economista,economista1,economista2,economista3,economista4]

    lista_out=[]
    for pato in galaxia:
        lista_out.extend(url_date(pato))

    return lista_out

def prepare_rss_usd():
    nytimes="http://rss.nytimes.com/services/xml/rss/nyt/Business.xml"
    nytimes1="http://www.nytimes.com/services/xml/rss/nyt/Dealbook.xml"
    nytimes2="http://rss.nytimes.com/services/xml/rss/nyt/Economy.xml"
    bidness="http://www.bidnessetc.com/businessnewsfeed/"
    deptransp="http://www.rita.dot.gov/bts/rss/press_releases.xml"
    nypost1="http://nypost.com/author/jonathon-m-trugman/feed/"
    nypost2="http://nypost.com/author/josh-kosman/feed/"
    nypost3="http://nypost.com/author/kevin-dugan/feed/"
    nypost4="http://nypost.com/author/michelle-celarier/feed/"
    cnbc="http://www.cnbc.com/id/10001147/device/rss/rss.html"
    zerohedge="http://www.zerohedge.com/fullrss2.xml"

    galaxia=[nytimes,nytimes1,nytimes2,bidness,deptransp,nypost1,nypost2,nypost3,nypost4,cnbc,zerohedge]

    lista_out=[]
    for pato in galaxia:
        lista_out.extend(url_date(pato))

    return lista_out


# In[12]:

def prepare_universal():
    urls=[]
    urls=[]
    texto=[]
    base_url='http://www.eluniversal.com.mx'
    pq = PyQuery('http://www.eluniversal.com.mx/cartera/economia/')

    #for item in pq(' .field-content a'):
    for item in pq('.field-content a'):
        href  = item.attrib['href']
        if href is None:
            continue
        if item.text is None:
            continue
        if len(item.text)<10:
            continue

        if base_url in href:
            urls.append(href)
            texto.append(item.text)
        else:
            urls.append(base_url+href)
            texto.append(item.text)

    liston_universal=list()

    for aux  in range(len(urls)):
        liston_universal.append((texto[aux],urls[aux]))
    return  liston_universal

def gimma_donald_df():
    today=datetime.datetime.now() - timedelta(days=5)
    today=today.date()
    names=['time','text']
    path1=os.path.join(settings.src_files, 'NAFTA4Vipin','stored_accounts')
    username='RealDonaldTrump'
    fName=path1+"/"+username+".txt"
    if not path.exists(fName):
        return None
#names=['Company','Last Sale','Change','Net%','Share Volume','News','url']
    pos_out=pd.DataFrame(columns=names)
    with open(fName) as f:
            for line in f:
                result = jsonpickle.decode(line)
                id_=result['id']
                pp=result['created_at']
                ff=datetime.datetime.strptime(pp,'%a %b %d %H:%M:%S +0000 %Y')
                gg=utc.localize(ff).astimezone(eastern)
                if gg.date()>=today:
                    #print result['text'],ff,gg,id_,gg.strftime("%Y-%m-%d %H:%M:%S %Z")
                    pos_out.loc[id_]=[gg.strftime("%Y-%m-%d %H:%M:%S %Z"),result['text']]



    pos_out.sort_index(inplace=True,ascending=False)

    return pos_out


def prepare_donald():
    donald_df=gimma_donald_df()

    if donald_df is None:
        return None
    else:
        source="Donald Trump at "
        liston_donald=list()
        limite=5
        contador=0
        for idx,row in donald_df.iterrows():
            if any(t in  row['text'].lower() for t in palabras):
                #print row['text']+" at "+row['time']
                liston_donald.append((row['text'],source+row['time']))
                if contador>limite:
                    break
                contador=contador+1
        return liston_donald










# In[13]:

def prepare_big_csv(big_liston):
    path=os.path.join(settings.src_files, 'NAFTA4Vipin','stored_news')
    today=datetime.datetime.now().date()
    df_out=pd.DataFrame(columns=['text','url'])
    contador=0
    for agency in big_liston:
        for news in agency:
            df_out.loc[contador]=news
            contador=contador+1

    df_out.to_csv(path+"/"+"TOP_NEWS_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')
    print "news done"






# In[17]:

def prepare_big_html(big_liston,color):
    total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u>  NAFTA INSIGHT 1345 </u></b></CAPTION>";
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


    tempo=prepare_html(big_liston[4],color[4])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem

    tempo=prepare_html(big_liston[5],color[5])
    if  len(tempo)>0:
        for elem in tempo:
            total=total+elem



    total = total + "</table></body></html>";

    return total

def prepare_big_html_new(big_liston,color):
    total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u>  NAFTA INSIGHT 1345 </u></b></CAPTION>";
    total = total + "<tr><td><b>" + "TITLE" + "</b><td width='230' align=center><b>"  + "SOURCE" + "</b></tr>";

    for kk in range(len(big_liston)):
        tempo=prepare_html(big_liston[kk],color[kk])
        if  len(tempo)>0:
            for elem in tempo:
                total=total+elem


    total = total + "</table></body></html>";

    return total



# In[18]:

def send_email_new(users,total):
	today=datetime.datetime.now()
	text="TOP NEWS "+today.strftime('%m/%d/%Y %H:%M')
	s = smtplib.SMTP("smtp.gmail.com",587)

	me = 'naftainsight1345@gmail.com'
	pwd='emerson1954'
	s.ehlo()
	s.starttls()
	s.login(me, pwd)

	you =users.split(',')
	msg = MIMEMultipart('alternative')
	msg['Subject'] =text
	msg['From'] = me
	#msg['To'] =[]
	#msg['Cc'] =[]
	#msg['Bcc'] =you

	#html=total
	part1 = MIMEText(text, 'plain')
	part2 = MIMEText(total, 'html','utf-8')

	# Attach parts into message container.
	# According to RFC 2046, the last part of a multipart message, in this case
	# the HTML message, is best and preferred.
	msg.attach(part1)
	msg.attach(part2)
	s.sendmail(me, you, msg.as_string().encode('ascii'))
	s.quit()
	print('email sent.')


# In[19]:

#brain
@sched.scheduled_job('cron', hour=8,minute=45,misfire_grace_time=60)
def timed_job():
# In[3]:
    liston_rss_usd=prepare_rss_usd()
    liston_rss_mxn=prepare_rss_mxn()
    liston_donald=prepare_donald()
    liston_bi=prepare_bi()
    liston_wsj=prepare_wsj()
    liston_bb=prepare_bb()
    liston_cnn=prepare_cnn()
    liston_financiero=prepare_financiero()
    liston_universal=prepare_universal()
    liston_economista=prepare_economista()





    if liston_donald is  None:
        big_liston=[liston_universal,liston_financiero,liston_economista,liston_rss_mxn,liston_bi,liston_cnn,liston_bb,liston_wsj,liston_rss_usd]
        color=['#809ff7','#f780e3','#f4e5c3','#f7bbe2','#ff9900','#00FF00','#00FFFF','#F3F781','#aeb6ef']
    else:
        big_liston=[liston_donald,liston_universal,liston_financiero,liston_economista,liston_rss_mxn,liston_bi,liston_cnn,liston_bb,liston_wsj,liston_rss_usd]
        color=['#aa9a06','#809ff7','#f780e3','#f4e5c3','#f7bbe2','#ff9900','#00FF00','#00FFFF','#F3F781','#aeb6ef']


    total=prepare_big_html_new(big_liston,color)

    #users="jose.pereza@me.com;vipin.anand.cpp@gmail.com;salumgreco@hotmail.com"
    users="jose.pereza@me.com,japerez20@gmail.com,salumgreco@hotmail.com,vipin.anand.cpp@gmail.com"
    #users='egreenfield@121send.com;Raphael_Kuenstle@gmx.net;jmcanchola@gmail.com;apina1411@gmail.com;jose.pereza@me.com;vipin.anand.cpp@gmail.com;japerez20@gmail.com;juanmanuelhec@gmail.com;salumgreco@hotmail.com;jtmaclay@yahoo.com'

    send_email_new(users,total)
    prepare_big_csv(big_liston)


# In[ ]:

sched.start()


# In[ ]:




# In[ ]:



