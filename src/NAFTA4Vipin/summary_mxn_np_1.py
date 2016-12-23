
# coding: utf-8

# In[1]:
#import sys
#import os.path

#sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import os, sys, re, pytz, datetime

import pandas as pd
#import pyquery
import unicodedata

from apscheduler.schedulers.blocking import BlockingScheduler
from bson import json_util



# In[2]:

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# In[3]:

from summa import summarizer
from summa import keywords


# In[4]:
import logging, traceback, settings
from url_processor import URLProcessor


from datetime import  timedelta

eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')


# In[5]:

from sklearn.feature_extraction.text import CountVectorizer
#import logging

# In[6]:
sched = BlockingScheduler()
logging.basicConfig()

strip_url = lambda url: re.sub('((www\.[\s]+)|(https?://[^\s]+))', "", url)


# In[7]:


def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
				  if unicodedata.category(c) != 'Mn')


# In[8]:

def remove_diacritic(input1):
	'''
	Accept a unicode string, and return a normal string (bytes in Python 3)
	without any diacritical marks.
	'''
	#return unicodedata.normalize('NFKD', input).encode('ASCII', 'ignore')
	#return unicodedata.normalize('NFD', input).encode('ASCII', 'ignore')
	s= ''.join((c for c in unicodedata.normalize('NFD',input1) if unicodedata.category(c) != 'Mn'))
	return s

#s = ''.join((c for c in unicodedata.normalize('NFD',unicode(mystr)) if unicodedata.category(c) != 'Mn'))


# In[9]:

twitter_ngram_splitter_case_sensitive = lambda a,b:  CountVectorizer(encoding='utf-8', stop_words=None, strip_accents=None, decode_error='ignore', analyzer='word', binary=True, max_df=1.0, max_features=None, ngram_range=(a,b), token_pattern='(?u)\\b\\w+\\b', lowercase=False, preprocessor=strip_url).build_analyzer()


# In[10]:

splitter=twitter_ngram_splitter_case_sensitive


# In[11]:

tokenizer = splitter(1,3)


# In[12]:

path1=os.path.join(settings.src_files, 'NAFTA4Vipin')


# In[13]:

#hca_file = os.path.join(settings.src_files, "priority_alerts", "models", "data_models", 'healthcare_action.json')
usd_file="catalogue_usd.json"
usd = {}
with open(path1+"//"+usd_file, 'r') as fp:
	usd = json_util.loads(fp.read().lower())


# In[14]:

mxn_file="catalogue_mxn.json"
mxn = {}
with open(path1+"//"+mxn_file, 'r') as fp:
	mxn = json_util.loads(fp.read().lower())


# In[ ]:




# In[15]:

#def remove_accents(data):
 #   return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.ascii_letters).lower()


# In[16]:


# In[17]:



# In[18]:

url_processor = URLProcessor()


# In[19]:

#from spanish guy
def remove_accents( mystr):
	"""Changes accented characters (áüç...) to their unaccented counterparts (auc...)."""
	s = ''.join((c for c in unicodedata.normalize('NFD',unicode(mystr)) if unicodedata.category(c) != 'Mn'))
	#return s.decode()
	return s


# In[22]:

def process_mexico_taxonomy(listota,mxn):

	trump_cats=[]
	for key in mxn:
		if len(mxn[key])>0:
				for word_text in listota:
					for word in mxn[key]:
						if word.lower()==word_text.lower():
							trump_cats.append(key)
							break
	return trump_cats





# In[23]:

def process_mexico_magic(listota,magic_words_mxn):

	trump_cats=[]
	for word in magic_words_mxn:
		for word_text in listota:
			if word.lower()==word_text.lower():
				trump_cats.append(word)
				break

	return trump_cats



# In[24]:

def score_jose_1(series):
	kw_ml=series['kw']
	kw_taxo=series['kw_taxo']
	kw_human=series['kw_magic']

	score=0
	ml=1
	taxo=10
	human=4
	if kw_ml is not None:
		score=score+ml*len(kw_ml)
	if kw_taxo is not None:
		score=score+taxo*len(set(kw_taxo))
	if kw_human is not None:
		score=score+human*len(set(kw_human))
	if 'trump' in kw_human:
		score=score+50

	return score
def score_jose_1_usd(series):
	kw_ml=series['kw']
	kw_taxo=series['kw_taxo']
	kw_human=series['kw_magic']

	score=0
	ml=1
	taxo=10
	human=30
	if kw_ml is not None:
		score=score+ml*len(kw_ml)
	if kw_taxo is not None:
		score=score+taxo*len(set(kw_taxo))
	if kw_human is not None:
		score=score+human*len(set(kw_human))


	return score





# In[25]:

def process_mexico(url):
	try:

		article=url_processor.process_url(url,'es')

		if article is None:

			return None
		results_jose=[]
		results_jose_magic_words=[]

		if len(article.text)<1:

			if len(article.title)>0:
				texto_no_accent=strip_accents(article.title)
				listota=tokenizer(texto_no_accent)
				results_jose=process_mexico_taxonomy(listota,mxn)
				results_jose_magic_words=process_mexico_magic(listota,magic_words_mxn)
				return [article.title,None,texto_no_accent.encode('utf-8'),results_jose,results_jose_magic_words]
			else:
				return None

		texto_no_accent=strip_accents(article.text)
		listota=tokenizer(texto_no_accent)
		results_jose=process_mexico_taxonomy(listota,mxn)
		results_jose_magic_words=process_mexico_magic(listota,magic_words_mxn)


		clave=keywords.keywords(article.text.encode('utf-8'),language='spanish',ratio=0.4)
		sumar=summarizer.summarize(article.text.encode('utf-8'),language='spanish',ratio=0.2)

		clave1=[i for i in clave.split('\n')]
		clave_lower=[x.lower() for x in clave1]

		if any(t in magic_words_mxn for t in clave_lower) or len(results_jose)>0 or len(results_jose_magic_words)>0:

			return [article.title,clave_lower,sumar,results_jose,results_jose_magic_words]
		else:
			return None

	except:
		if any(t in magic_words_mxn for t in article.text.encode('utf-8')) or len(results_jose)>0 or len(results_jose_magic_words)>0:

			return [article.title,None,article.text.encode('utf-8'),results_jose,results_jose_magic_words]
		else:
			return None






# In[26]:

def process_usa(url):
	try:

		article=url_processor.process_url(url)
		if article is None:
			return None

		results_jose=[]
		results_jose_magic_words=[]


		if len(article.text)<1:
			if len(article.title)>0:

				texto_no_accent=article.title
				listota=tokenizer(article.title)
				results_jose=process_mexico_taxonomy(listota,usd)
				results_jose_magic_words=process_mexico_magic(listota,magic_words_usd)
				return [article.title,None,texto_no_accent,results_jose,results_jose_magic_words]
			else:
				return None
		texto_no_accent=article.text
		listota=tokenizer(texto_no_accent)
		results_jose=process_mexico_taxonomy(listota,usd)
		results_jose_magic_words=process_mexico_magic(listota,magic_words_usd)


		clave=keywords.keywords(article.text,language='english',ratio=0.4)
		sumar=summarizer.summarize(article.text,language='english',ratio=0.2)

		clave1=[i for i in clave.split('\n')]
		clave_lower=[x.lower() for x in clave1]
		if any(t in magic_words_usd for t in clave_lower) or len(results_jose)>0 or len(results_jose_magic_words)>0:

			return [article.title,clave_lower,sumar,results_jose,results_jose_magic_words]
		else:
			return None

	except:
		if any(t in magic_words_usd for t in article.text) or len(results_jose)>0 or len(results_jose_magic_words)>0:

			return [article.title,None,article.text,results_jose,results_jose_magic_words]
		else:
			return None




# In[27]:

#we select how many days, including today ,to the past we want to create the analysis.
#if we select zero we will consider only news for today
#it returns the dataframe that contains all the news for the most recent dasy_2 (including today)
def create_history_df(days_2_past,path3):
	today=datetime.datetime.now() - timedelta(days=days_2_past)
	today=today.date()
	selected=[]
	#
	for name in os.listdir(path3):
		#for name in files
			try:
				if("Store") in name:
					continue
				fecha_str=name.split("_")[-1].split(".")[0]
				fecha_python=datetime.datetime.strptime(fecha_str,'%m%d%Y').date()
				if fecha_python>=today:
					selected.append(name)
			except Exception as e:
				sys.stderr.write('\n\t' + str(traceback.print_exc()))
				logging.error(e, exc_info=1)
	df=pd.DataFrame()
	for elem in selected:

		tf=pd.read_csv(path3+"//"+elem)
		df=df.append(tf, ignore_index=True)

	df1=df.drop_duplicates(['url'], keep='last')
	return df1

	#df=pd.read_csv(path3+"//"+selected[0])
	#once we have the name of those files we proceed to append them in a datafram





# In[28]:

def process_and_prepare_usa(df,score_jose_1):
	gf=pd.DataFrame(columns=['title','summary','kw','kw_taxo','kw_magic','url'])
	contador=0
	for idx,row in df.iterrows():
		if row['url'] is None:
			continue
		if ".mx" not in row['url']:

			result_mx=process_usa(row['url'])
			if result_mx is  not None:

				temp=result_mx[2]
				gf.loc[contador]=[result_mx[0],temp,result_mx[1],result_mx[3],result_mx[4],row['url']]
				contador=contador+1

	gf['score_1']=gf.apply(score_jose_1_usd,axis=1)
	out_gf = gf.sort('score_1', ascending=False)
	bb=out_gf.drop_duplicates(['title'], keep='last')
	return bb


# In[29]:

def process_and_prepare(df,score_jose_1):
	gf=pd.DataFrame(columns=['title','summary','kw','kw_taxo','kw_magic','url'])
	contador=0
	for idx,row in df.iterrows():
		if ".mx" in row['url']:
			result_mx=process_mexico(row['url'])
			if result_mx is  not None:

				temp=strip_accents(unicode(result_mx[2], "utf-8"))
				gf.loc[contador]=[result_mx[0],temp,result_mx[1],result_mx[3],result_mx[4],row['url']]
				contador=contador+1
	gf['score_1']=gf.apply(score_jose_1,axis=1)
	out_gf = gf.sort('score_1', ascending=False)
	bb=out_gf.drop_duplicates(['title'], keep='last')
	return bb



# In[30]:

#These are the words at the top level to filter all the possible text
magic_words_mxn=['trumpenstein','fitch','tarifas','tlcan','tlc','hacienda','banxico','donald','trump','china','peso','volatilidad','eu','banqueros','tasas','exportar','importar']
magic_words_usd=['trump','mexico','mexicans','mexican','trumpenstein']


# In[31]:

path2=os.path.join(settings.src_files, 'NAFTA4Vipin','stored_accounts')
path3=os.path.join(settings.src_files, 'NAFTA4Vipin','stored_news')


# In[32]:

def prepare_html_df(suma,title,color,sec_color):
	#color="#F7BE81"
	#sec_color="#f7bbe2"

	out1_jose = "<tr><td BGCOLOR="+color+">" + title + "<td align=center BGCOLOR="+sec_color+">" + suma +  "</tr>";
	return  out1_jose




# In[33]:

def prepare_kw(row):
	una=row['kw']
	dos=row['kw_taxo']
	tres=row['kw_magic']
	out_lista=[]
	if dos is not None:
		dos=list(set(dos))
		out_lista.extend(dos)
	if una is not None:
		una=list(set(una))
		out_lista.extend(una)
	if tres is not None:
		tres=list(set(tres))
		out_lista.extend(tres)
	return out_lista









# In[39]:

def prepare_big_html_df(data):
	total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u> NAFTA INSIGHT 1345 </u></b></CAPTION>";
	total = total + "<tr><td align=center><b>" + "TITULO" + "</b><td width='230' align=center><b>"  + "RESUMEN" + "</b></tr>";

	#for kk in range(len(big_liston)):
	for idx,row in data.iterrows():

		#<font color="red">This is some text!</font>
		calif=" Score: "+str(row['score_1'])
		aux_calif='<font color="green">'+calif+'</font>'
		keywords_mxn=prepare_kw(row)
		kiki="Palabras Clave: "
		talla=min(len(keywords_mxn),4)
		for aux in range(talla):
			kiki=kiki+keywords_mxn[aux]+","
		aux_kiki='<font color="brown">'+kiki+'</font>'
		#aux_title='<font color=#070706>'+row['title']+'</font>'
		aux_title='<strong>'+row['title']+'</strong>'

		tempo=prepare_html_df(row['summary']+" "+row['url'],aux_title+aux_calif+" "+aux_kiki)
		if  len(tempo)>0:
			total=total+tempo


	total = total + "</table></body></html>";

	return total


# In[40]:

def prepare_big_html_df_combo(data_mxn,data_usd):
	total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u> TEST NAFTA INSIGHT 1345 </u></b></CAPTION>";
	total = total + "<tr><td align=center><b>" + "TITULO" + "</b><td width='230' align=center><b>"  + "RESUMEN" + "</b></tr>";
	#color="#ed80f7"
	color="#f780dd"
	sec_color="#f7bbe2"
	#color="#ed80f7"
	#color="#F7BE81"
	#for kk in range(len(big_liston)):
	for idx,row in data_mxn.iterrows():
		calif=" Score: "+str(row['score_1'])
		aux_calif='<font color="green">'+calif+'</font>'
		keywords_mxn=prepare_kw(row)
		kiki="Palabras Clave: "
		talla=min(len(keywords_mxn),4)
		for aux in range(talla):
			kiki=kiki+keywords_mxn[aux]+" "
		aux_kiki='<font color="brown">'+kiki+'</font>'

		aux_title='<strong>'+row['title']+'</strong>'

		tempo=prepare_html_df(row['summary']+" "+row['url'],aux_title+aux_calif+" "+aux_kiki,color,sec_color)
		if  len(tempo)>0:
			total=total+tempo
	color="#80a2f7"
	sec_color="#aeb6ef"
	for idx,row in data_usd.iterrows():
		calif=" Score: "+str(row['score_1'])
		aux_calif='<font color="green">'+calif+'</font>'
		keywords_mxn=prepare_kw(row)
		kiki="Keywords: "
		talla=min(len(keywords_mxn),4)
		for aux in range(talla):
			kiki=kiki+keywords_mxn[aux]+" "
		aux_kiki='<font color="brown">'+kiki+'</font>'

		aux_title='<strong>'+row['title']+'</strong>'

		tempo=prepare_html_df(row['summary']+" "+row['url'],aux_title+aux_calif+" "+aux_kiki,color,sec_color)
		if  len(tempo)>0:
			total=total+tempo



	total = total + "</table></body></html>";
	return total


# In[42]:

def send_email_new(users,total):
	today=datetime.datetime.now()
	text="TOP NEWS MEXICO USA "+today.strftime('%m/%d/%Y %H:%M')
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


# In[266]:

#MXN
#total=prepare_big_html_new(big_liston,color)
#total=prepare_big_html_df(out_gf)
#days_2_past=1
#df_history=create_history_df(days_2_past,path3)
#df_prepared=process_and_prepare(df_history,score_jose_1)
#total=prepare_big_html_df(df_prepared)

#users="jose.pereza@me.com;vipin.anand.cpp@gmail.com;salumgreco@hotmail.com"
#users="jose.pereza@me.com;japerez20@gmail.com;salumgreco@hotmail.com;vipin.anand.cpp@gmail.com;"
#users='egreenfield@121send.com;Raphael_Kuenstle@gmx.net;jmcanchola@gmail.com;apina1411@gmail.com;jose.pereza@me.com;vipin.anand.cpp@gmail.com;japerez20@gmail.com;juanmanuelhec@gmail.com;salumgreco@hotmail.com;jtmaclay@yahoo.com'

#send_email_new(users,total)


# In[268]:

#USA
#days_2_past=1
#df_history=create_history_df(days_2_past,path3)
#process_and_prepare_usa
#df_prepared=process_and_prepare_usa(df_history,score_jose_1)
#total=prepare_big_html_df(df_prepared)
#df_prepared=process_and_prepare(df_history,score_jose_1)
#users="jose.pereza@me.com;japerez20@gmail.com;salumgreco@hotmail.com;vipin.anand.cpp@gmail.com;"
#send_email_new(users,total)


# In[37]:

#combo
@sched.scheduled_job('cron', hour=15,minute=05,misfire_grace_time=60)
def timed_job():
	days_2_past=0
	df_history=create_history_df(days_2_past,path3)
	#process_and_prepare_usa
	df_prepared_mxn=process_and_prepare(df_history,score_jose_1)
	df_prepared_usa=process_and_prepare_usa(df_history,score_jose_1)
	total=prepare_big_html_df_combo(df_prepared_mxn,df_prepared_usa)

	#df_prepared=process_and_prepare(df_history,score_jose_1)
	users="jose.pereza@me.com,japerez20@gmail.com,salumgreco@hotmail.com,vipin.anand.cpp@gmail.com;"
	#users='anahik7@gmail.com,egreenfield@121send.com,Raphael_Kuenstle@gmx.net,jmcanchola@gmail.com,apina1411@gmail.com,jose.pereza@me.com,vipin.anand.cpp@gmail.com,japerez20@gmail.com,juanmanuelhec@gmail.com,salumgreco@hotmail.com,jtmaclay@yahoo.com'

	send_email_new(users,total)


sched.start()


