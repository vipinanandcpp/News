
# coding: utf-8

# In[1]:

#this isa warm up notebook. mainly to play
import tweepy
import sys
import jsonpickle
import os
import re
from os import path
from apscheduler.schedulers.blocking import BlockingScheduler

import settings

sched = BlockingScheduler()
# In[25]:

#API_KEY="9MCaTl4jLoQtQeyL0buwqiuhI"
#API_SECRET="wC9yGlJkutPkYnR7t041mENCJ83ITRWdowIjjXYjuXQC5yoJb1"
API_KEY="qpZ2cmntw0fGcm0HSABSBLcWA"
API_SECRET="31f7HkPPpCCEuVzC1FWEgRZLI0f06UQo3Ax80qopjxdzJ0gm3B"



# In[26]:

auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True,
				   wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)


# In[23]:

#u need to provide the path where u want to store the initial set.
def prepare_initial_set(username,path):
    maxTweets = 10000000 # Some arbitrary large number
    tweetsPerQry = 100  # this is the max the API permits
    sinceId = None
    max_id = int(-1)
    fName=path+"/"+username+".txt"
    tweetCount = 0
    print("Downloading max {0} tweets".format(maxTweets))
    with open(fName, 'w') as f:
        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry)
                    else:
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry,since_id=sinceId)
                else:
                    if (not sinceId):
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry,max_id=str(max_id - 1))
                    else:
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry, since_id=sinceId)
                if not new_tweets:
                    print("No more tweets found")
                    break
                for tweet in new_tweets:
                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) +'\n')
                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

    print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))




# In[10]:

def give_me_sinceid(username,path):
    lista=[]
    fname=path+"/"+username+".txt"
    with open(fname) as f:
        for line in f:
            result = jsonpickle.decode(line)
            lista.append(result['id'])

    lista.sort()
    return lista[-1]




# In[11]:

#once the original file is stored we proceed to store onn regular basis.
def start_storing(username,path):
    fName=path+"/"+username+".txt"
    sinceId=give_me_sinceid(username,path)
    maxTweets = 10000000 # Some arbitrary large number
    tweetsPerQry = 100  # this is the max the API permits
    max_id = int(-1)
    tweetCount = 0
    print("Downloading max {0} tweets".format(maxTweets))
    with open(fName, 'a') as f:
        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry)
                    else:
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry,since_id=sinceId)
                else:
                    if (not sinceId):
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry,max_id=str(max_id - 1))
                    else:
                        new_tweets=api.user_timeline(screen_name=username,count=tweetsPerQry, since_id=sinceId)
                if not new_tweets:
                    print("No more tweets found")
                    break
                for tweet in new_tweets:
                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) +'\n')
                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
                sinceId= new_tweets[0].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

    print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))


#if __name__ == "__main__":
@sched.scheduled_job('cron', hour=0,minute=0,misfire_grace_time=60)
def timed_job():
    #path1='S:\\Quant\\JAPEREZ\\TOP_ACCOUNTS'
    path1=os.path.join(settings.src_files, 'NAFTA4Vipin','stored_accounts')
    username='OpenOutCrier'
    PATHtest=path1+"/"+username+".txt"
    if path.exists(PATHtest) and path.isfile(PATHtest):
        start_storing(username,path1)
    else:
        prepare_initial_set(username,path1)

    username='RealDonaldTrump'
    PATHtest=path1+"/"+username+".txt"
    if path.exists(PATHtest) and path.isfile(PATHtest):
        start_storing(username,path1)
    else:
        prepare_initial_set(username,path1)



sched.start()

