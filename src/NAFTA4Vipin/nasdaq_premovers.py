
# coding: utf-8

# In[8]:
from apscheduler.schedulers.blocking import BlockingScheduler
#from datetime import datetime
import time
import datetime
#import pywintypes

from pyquery import PyQuery
import pandas as pd

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# In[9]:

#import win32com.client
import numpy as np
import collections
#import pythoncom

import logging


# In[10]:

import tweepy
import sys
import jsonpickle
import os
import re
from collections import Counter


# In[11]:

import os, datetime, pytz
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

import logging, settings

sched = BlockingScheduler()
logging.basicConfig()


# In[12]:


API_KEY="qpZ2cmntw0fGcm0HSABSBLcWA"
API_SECRET="31f7HkPPpCCEuVzC1FWEgRZLI0f06UQo3Ax80qopjxdzJ0gm3B"


if not os.path.exists(os.path.join(settings.src_files, 'NAFTA4Vipin','NASDAQ')):
    os.makedirs(os.path.join(settings.src_files, 'NAFTA4Vipin','NASDAQ'))

# In[13]:

auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)


# In[14]:

strip_url = lambda url: re.sub('((www\.[\s]+)|(https?://[^\s]+))', "", url)
url_matcher = re.compile(r'https?:\/\/.*[\r\n]*')
url_regex = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
symbols_finder = re.compile("\$([A-Z]+)")


# In[15]:

def remove_url(text):
	return url_matcher.sub('', text)


# In[16]:

def gimma_url(text):
    return re.search("(?P<url>https?://[^\s]+)",text).group("url")


# In[17]:

def gimma_all_url_poke(text):
    return re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)


# In[18]:

#this processing returns only the users that belong to total_users
def process_big_dic(big_dic,total_users):
    big_dic_out={}
    for symbol in big_dic.keys():
        dic_temp={}
        for user in  big_dic[symbol].keys():
            if user in total_users:

                dic_temp[user]=big_dic[symbol][user]
        big_dic_out[symbol]=dic_temp
    return big_dic_out


# In[19]:

impo_users={"reutersdeals", "d4ytrad3", "financialjuice", "OptionsHawk", "AlipesNews", "OpenOutcrier", "alpha_maven", "LaMonicaBuzz", "BloombergTV", "WSJ", "Benzinga", "WSJMoneyBeat", "DAMSConsulting", "WSJmarkets", "MarketNewswire", "Selerity", "CNBCWorld", "theflynews", "TheStreet", "ReutersBiz", "DowJones", "carlquintanilla", "joseResearch", "AndyBiotech", "zbiotech", "adamfeuerstein", "MarketCurrents", "SEEKINGALPHA_FS", "Briefingcom", "ForTraders", "BVFinancials", "BVTechnology", "BVUtilities", "BV_Services ", "BV_Industrial", "BV_Healthcare", "BVConsumerGoods", "BVConglomerates", "BVBasicMaterial", "YahooFinance", "SAlphaAAPL", "zerohedge", "SeekingAlpha", "SAlphaTrending", "tsilver_apex", "FonsieTrader", "AmericanBanking", "SleekMoneycom", "SpeedyCalls"}
healthcare={"itrader75", "FZucchi", "Biotech2050", "BioTerp", "MartinShkreli", "RNAiAnalyst", "pharmamaven", "Biotech2015", "Jacob_Mintz", "lancejepsen", "odibro", "CatalystWatcher", "GNWLive", "bored2tears", "MarketCurrents", "BriefingCom", "SA_HealthInvest", "TheIpHawk", "skaushi", "pharmalot", "Mykalt45", "ScripDonnaDC", "CNBCnow", "eperlste", "skaushi", "SarahKarlin", "JacobPlieth", "lisamjarvis", "Mykalt45", "FonsieTrader", "BioMedSector", "BioPharmaDive", "theflynews", "financialjuice", "pnani456", "MookTrader", "Hypocrites_Oath", "d4ytrad3", "dsobek", "OpenOutcrier", "ScripDonnaDC", "BioRunUp", "BioStocks", "d4ytrad3", "DAMSConsulting", "BenzingaPro", "JohnCFierce9", "ScripDonnaDC9", "TradeHawk", "BioMedSector", "w_biltmore85", "maureenmfarrell", "ThudderWicks", "bradloncar", "JPZaragoza1", "Boston_Biotech", "fwpharma", "El_Sequenco", "WrigleyTom", "SudhanvaRaj", "JoeHsieh24", "ehITM", "daytradedon", "sfef84", "portefeuillefun", "d4ytrad3",
				"OpenOutcrier",
				"WallStJesus", "zbiotech", "tgtxdough", "bio_clouseau", "adamfeuerstein", "princetongb", "cmtstockcoach", "AndyBiotech", "AF_biotech", "zDonShimoda", "JNapodano", "bradloncar", "JPZaragoza1", "TylerHCanalyst", "adamfeuerstein", "AndyBiotech", "zbiotech", "princetongb", "tgtxdough", "bio_clouseau", "bradloncar"}

mergers={"DonutShorts", "ScottMAustin", "bristei", "BrianSozzi", "advdesk", "alpepinnazzo", "Opinterest", "ACInvestorBlog", "Benzinga", "BenzingaPro", "BioBreakout", "BioRunUp", "BioStocks", "BloombergDeals", "Briefingcom", "CNBC", "CNBCSocial", "CNBCWorld", "CNBCnow", "DanaMattioli", "EricPFiercel", "FiercePharma", "GregRoumeliotis", "HiddenGemTrader", "Hypocrites_Oath", "JeeYeonParkCNBC", "Lebeaucarnews", "LianaBaker", "MarketCurrents", "MichaelStone", "NOD008", "OpenOutcrier", "Opinterest", "OptionsHawk", "OptionsTrader31", "QuoththeRavenSA", "Recode", "ScripMandy", "SheerazRaza", "SquawkCNBC", "Street_Insider", "SwatOptions", "TheDomino", "ThudderWicks", "TradeableAlerts", "WSJAsia", "WSJbusiness", "WSJdeals", "WallStChatter", "WrigleyTom", "adamfeuerstein", "ahess247", "bman_alerts", "bored2tears", "briefingcom", "carlquintanilla", "d4ytrad3", "danacimilluca", "davidfaber", "ehITM", "financialjuice", "kaylatausche", "lilnickysmith",
				"lisamjarvis", "livesquawk", "markflowchatter", "megtirrell", "mooktrader", "natebecker", "ozoran", "pnani456", "tradermarket247", "tsilver_apex", "zbiotech", "CaptainFuture__", "Opinterest", "PreetaTweets", "NatalieGrover", "ymscapital", "theflynews", "SquawkStreet", "sonalibasak", "ldelevingne", "taralach", "BarbarianCap", "jonathanrockoff", "JohnCFierce", "nixon786", "DamianFierce", "GNWLive", "marlentweets", "SashaDamouni", "DougLavanture", "BrianSozzi", "VictoriaCraig", "blsuth", "mikeesterl", "sfef84", "TradeHawk", "DAMSConsulting", "WSJbreakingnews", "ReutersBiz", "nypostbiz", "nypost", "adamfeuerstein", "DamianFierce", "OMillionaires", "SA_Mergers", "brucejapsen", "BioBreakout", "pnani456", "Street_Insider"}

verified={"theflynews", "thestreetalerts", "AlipesNews", "financialjuice", "Selerity", "BloombergTV", "fonsietrader", "cnbc", "zerohedge", "alpienews", "ForTraders", "ratingsnetwork", "NASDAQODUK", "SeekingAlpha", "Carl_C_Icahn", "benzinga", "elonmusk", "bloomberg", "business", "reuters", "ap", "wsj", "nytimes", "JNJNews", "MarketWatch", "DailyFXTeam", "FXStreetNews", "Briefingcom", "50Pips", "stocknews77", "stocknews99", "stocknews247", "snn_team", "stockwire24", "stock_newsnet24", "FXStreetReports", "ForexNewsMole", "RatingsNetwork", "BenzingaMedia", "MarketNewswire", "ConsumerFeed", "The_Analyst", "MarketCurrents", "financialpost", "KeithMcCullough", "SwingTradeAlert"}
vip_users = { "DanaMattioli", "jonathanrockoff", "d4ytrad3", "OpenOutcrier", "DAMSConsulting", "QTRResearch" };
garbagge={"stockstobuy"}

# In[20]:

total_users=set.union(impo_users,healthcare,mergers,verified,vip_users)


# In[21]:

import sys, logging



headers = {
		'browser_user_agent': 'Mozilla'
	}


# In[39]:

def select_top(easy_df_today):
    ff=Counter(easy_df_today['symbol'].values)
    diction={}
    for tico in ff.keys():
        temp_df= easy_df_today[ easy_df_today['symbol']==tico]
        if 'OpenOutcrier' in temp_df['user'].values:
            oo=temp_df[temp_df['user']=='OpenOutcrier']
            diction[tico]=[oo.iloc[0]['title'],oo.iloc[0]['url']]
        else:
            for idx,row in temp_df.iterrows():
                if row['url'] is None:
                    diction[tico]=[row['title'],row['url']]
                else:
                     diction[tico]=[row['title'],row['url']]
                     break
    return diction



# In[70]:

def prepare_html_table(lista_big):
	total = "<html><body><table border=5 BORDERCOLOR=BLUE><CAPTION><b><u> MOST ADVANCED, MOST DECLINED  PRE MARKET </u></b></CAPTION>";

	#total = total + "<tr><td><b>" + "SYMBOL" + "</b><td width='130' align=center><b>" + "PREV CLOSE CHANGE" + "</b><td width='130'><b>" + "SOURCE" + "</b><td width='200'><b>" + "TIMESTAMP" + "</b><td><b>" + "SUBJECT" + "</b></tr>";
	total = total + "<tr><td><b>" + "SYMBOL" + "</b><td><b>" + "COMPANY" + "</b><td><b>" + "LAST SALE" + "</b><td><b>" + "CHANGE" + "</b><td><b>" +"Net%"+"</b><td><b>"+"Share Volume"+"</b><td><b>"+ "NEWS"+"</b><td><b>"+"URL"+ "</b></tr>";
	for elem in lista_big:
			total=total+elem
	total = total + "</table></body></html>";

	return total


# In[67]:

def prepare_tables():
    url='http://www.nasdaq.com/extended-trading/premarket-mostactive.aspx'
    doc = PyQuery(url)
    #this is the brain
    table_dic={}
    contador=1
    for tabla in doc('table').items():
        values ={}
        listona=list()
        for rows in tabla('td').items():
            if rows('a'):
                title=rows('a').text()
                values[title]=list()

            else:
                values[title].append(rows.text())

        table_dic[str(contador)]=values
        contador=contador+1
    return table_dic



# In[25]:

def return_pos_neg(table_dic):
    pos_df=pd.DataFrame(columns=['Company','Last Sale','Change','Net%','Share Volume'])
    for simb in table_dic[str(5)].keys():
        row=table_dic[str(5)][simb]
        simbolo=simb
        company=row[0]
        sale=row[2]
        temp=row[3].split()
        change=temp[0]
        net=temp[2]
        volume=row[4]
        pos_df.loc[simb]=[company,sale,change,net,volume]

    neg_df=pd.DataFrame(columns=['Company','Last Sale','Change','Net%','Share Volume'])
    for simb in table_dic[str(6)].keys():
        row=table_dic[str(6)][simb]
        simbolo=simb
        company=row[0]
        sale=row[2]
        temp=row[3].split()
        change=temp[0]
        net=temp[2]
        volume=row[4]
        neg_df.loc[simb]=[company,sale,change,net,volume]

    #pos_df_sorted=pos_df.sort(['Net%'],ascending=False)
    #neg_df_sorted=neg_df.sort(['Net%'],ascending=False)
    pos_df['Neto']=pos_df['Net%'].str.rstrip('%').astype('float64')
    neg_df['Neto']=neg_df['Net%'].str.rstrip('%').astype('float64')

    pos_df_sorted=pos_df.sort_values(['Neto'],ascending=False)
    neg_df_sorted=neg_df.sort_values(['Neto'],ascending=False)




    return pos_df_sorted,neg_df_sorted



# In[37]:

#here i select only those messages that are for today
def gimma_easy_df(big_dic_proc):
    easy_df_today=pd.DataFrame(columns=['symbol','user','time','title','url'])
    today=datetime.datetime.now().date()
    i=0
    for ticker in big_dic_proc.keys():
        for user in big_dic_proc[ticker].keys():
            for fecha in big_dic_proc[ticker][user].keys():

                if(fecha.date()<today):
                    continue
                objecto=big_dic_proc[ticker][user][fecha]

                #fecha_out=fecha.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                fecha_out=fecha.strftime("%Y-%m-%d %H:%M:%S %Z")
                #print fecha,fechat,exe

                #print objecto.text
                url=gimma_all_url_poke(objecto.text)
                if len(url)>0:
                    easy_df_today.loc[i]=[ticker,user,fecha_out,remove_url(objecto.text),url[0]]
                else:
                     easy_df_today.loc[i]=[ticker,user,fecha_out,objecto.text,None]
                i=i+1
    return easy_df_today


# In[32]:

def hit_twitter(total_users,tickers):
    #hit twitter!!!!
    big_dic={}
    counter=0
    for simbolo in tickers:
        dic_users={}
        tico="$"+simbolo.strip()
        new_tweets=api.search(q=tico,count=100)
        for elem in new_tweets:
            if elem.user.screen_name not in dic_users.keys():
                dic_users[elem.user.screen_name]=dict()

            tempo=dic_users[elem.user.screen_name]
            pp=elem.created_at
            tempo[utc.localize(pp).astimezone(eastern)]=elem
        big_dic[simbolo]=dic_users
        counter=counter+1
        #if counter>3:
         #   break
    #today=datetime.datetime.now().date()
    big_dic_proc=process_big_dic(big_dic,total_users)
    easy_df_today=gimma_easy_df(big_dic_proc)
    return easy_df_today





# In[34]:

def return_html(pos_df_sorted,neg_df_sorted,top):

    lista_big=[]
    for idx ,row in pos_df_sorted.iterrows():
        if idx in top.keys():
            news=top[idx][0]
            url="&nbsp"
            if top[idx][1] is not None:
                url=top[idx][1]
        #out1_jose = "<tr><td BGCOLOR=#00ff40>" +row['symbol'] + "<td align=center>" + cambio_str + "<td>" + "TEST" + "<td>" + str(msg.CreationTime) + "<td>" + msg.Subject.replace("*","") + "</tr>";
            out1_jose = "<tr><td BGCOLOR=#00ff40>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" + news+"<td>" + url+ "</tr>";
        else:
            out1_jose = "<tr><td BGCOLOR=#00ff40>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" +"&nbsp"+"<td>" +"&nbsp"+ "</tr>";
        lista_big.append(out1_jose)

    for idx ,row in neg_df_sorted.iterrows():
        if idx in top.keys():
            news=top[idx][0]
            url="&nbsp"
            if top[idx][1] is not None:
                url=top[idx][1]
        #out1_jose = "<tr><td BGCOLOR=#00ff40>" +row['symbol'] + "<td align=center>" + cambio_str + "<td>" + "TEST" + "<td>" + str(msg.CreationTime) + "<td>" + msg.Subject.replace("*","") + "</tr>";
            out1_jose = "<tr><td BGCOLOR=#ff0000>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" + news+"<td>" + url+ "</tr>";
        else:
            out1_jose = "<tr><td BGCOLOR=#ff0000>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" +"&nbsp"+"<td>" +"&nbsp"+ "</tr>";
        lista_big.append(out1_jose )
    return lista_big


def pos_neg_2_print(pos_df_sorted,neg_df_sorted,top):
    names=['Company','Last Sale','Change','Net%','Share Volume','News','url']
    pos_out=pd.DataFrame(columns=names)
    neg_out=pd.DataFrame(columns=names)

    lista_big=[]
    for idx ,row in pos_df_sorted.iterrows():
        if idx in top.keys():
            news=top[idx][0]
            url=None
            if top[idx][1] is not None:
                url=top[idx][1]
        #out1_jose = "<tr><td BGCOLOR=#00ff40>" +row['symbol'] + "<td align=center>" + cambio_str + "<td>" + "TEST" + "<td>" + str(msg.CreationTime) + "<td>" + msg.Subject.replace("*","") + "</tr>";
            #out1_jose = "<tr><td BGCOLOR=#00ff40>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" + news+"<td>" + url+ "</tr>";
            lista=[ row['Company'] , row['Last Sale'] ,row['Change'] ,row['Net%'] , row['Share Volume'],news, url]

        else:
            #out1_jose = "<tr><td BGCOLOR=#00ff40>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" +"&nbsp"+"<td>" +"&nbsp"+ "</tr>";
            lista=[ row['Company'] , row['Last Sale'] ,row['Change'] ,row['Net%'] , row['Share Volume'],None, None]
        #lista_big.append(out1_jose )
        pos_out.loc[idx]=lista

    for idx ,row in neg_df_sorted.iterrows():
        if idx in top.keys():
            news=top[idx][0]
            url=None
            if top[idx][1] is not None:
                url=top[idx][1]
        #out1_jose = "<tr><td BGCOLOR=#00ff40>" +row['symbol'] + "<td align=center>" + cambio_str + "<td>" + "TEST" + "<td>" + str(msg.CreationTime) + "<td>" + msg.Subject.replace("*","") + "</tr>";
            #out1_jose = "<tr><td BGCOLOR=#ff0000>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" + news+"<td>" + url+ "</tr>";
            lista=[ row['Company'] , row['Last Sale'] ,row['Change'] ,row['Net%'] , row['Share Volume'],news, url]
        else:
            #out1_jose = "<tr><td BGCOLOR=#ff0000>" +idx + "<td >" + row['Company'] + "<td>" + row['Last Sale'] + "<td>" + row['Change'] + "<td>" + row['Net%'] +"<td>" + row['Share Volume']+"<td>" +"&nbsp"+"<td>" +"&nbsp"+ "</tr>";
            lista=[ row['Company'] , row['Last Sale'] ,row['Change'] ,row['Net%'] , row['Share Volume'],None, None]
        #lista_big.append(out1_jose )
        neg_out.loc[idx]=lista
    return pos_out,neg_out


# In[73]:






# In[74]:
@sched.scheduled_job('cron', day_of_week='sat-sun', hour=8,minute=30,misfire_grace_time=600)
def timed_job():
#main
    table_dic=prepare_tables()
    pos_sorted,neg_sorted=return_pos_neg(table_dic)
    tickers=pos_sorted.index.tolist()
    tickers.extend(neg_sorted.index.tolist())
    easy_df_today=hit_twitter(total_users,tickers)
    top=select_top(easy_df_today)
    lista_big=return_html(pos_sorted,neg_sorted,top)

    total=prepare_html_table(lista_big)

    users='jose.pereza@me.com;vipin.anand.cpp@gmail.com'
    send_email_new_gmail(users,total)

    today=datetime.datetime.now().date()
    path=os.path.join(settings.src_files, 'NAFTA4Vipin','NASDAQ')
    #path='put your vipin path here'

    cmd=os.getcwd()
    #pos_sorted.to_csv(cmd+"\\"+"top_movers"+"\\"+"positive_"+today.strftime('%m%d%Y')+".csv")
    #neg_sorted.to_csv(cmd+"\\"+"top_movers"+"\\"+"negative_"+today.strftime('%m%d%Y')+".csv")

    #print for my numerical experiemnts
    pos_out,neg_out=pos_neg_2_print(pos_sorted,neg_sorted,top)

    pos_out.to_csv(path+"/"+"positive_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')
    neg_out.to_csv(path+"/"+"negative_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')


    #pos_out.to_csv(cmd+"\\"+"top_movers"+"\\"+"positive_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')
    #neg_out.to_csv(cmd+"\\"+"top_movers"+"\\"+"negative_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')

    #easy_df_today.to_csv(cmd+"\\"+"top_movers"+"\\"+"infoall_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')

    easy_df_today.to_csv(path+"/"+"infoall_"+today.strftime('%m%d%Y')+".csv",encoding='utf-8')

    print(cmd)
    #print(cmd+"\\"+"top_movers"+"\\"+"positive_"+today.strftime('%m%d%Y')+".csv")


def send_email_new_gmail(users,total):
    today=datetime.datetime.now()
    text="TOP Movers "+today.strftime('%m/%d/%Y %H:%M')
    s = smtplib.SMTP("smtp.gmail.com",587)

    me = 'naftainsight1345@gmail.com'
    pwd='emerson1954'
    s.ehlo()
    s.starttls()
    s.login(me, pwd)

    you =users
    msg = MIMEMultipart('alternative')
    msg['Subject'] =text
    msg['From'] = me
    #msg['Cc'] =[]
    #msg['Bcc'] =you

    html=total
    #part1 = MIMEText(text, 'plain')
    #part2 = MIMEText(total, 'html','utf-8')
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html','utf-8')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)
    s.sendmail(me, you, msg.as_string().encode('ascii'))
    s.quit()
    print('email sent.')
    #s.sendmail(me, you.split(";"), msg.as_string())
    #s.quit()

    #print('email sent.')





sched.start()






# In[59]:

#most active is 4, most advanced is 5, most declined is 6


# In[ ]:

#at this moment I have the positive movers, negative movers and the news in easy_df_today. Now I need to proceed to create the report



# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



