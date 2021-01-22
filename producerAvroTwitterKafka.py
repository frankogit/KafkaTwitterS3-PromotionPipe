
#!/usr/bin/env python
import twitter
import csv
import time
import traceback
import pandas as pd
from pandas import DataFrame
from datetime import datetime, date, timedelta
import sys, importlib,os
import warnings
import logging
#from confluent_kafka import Producer
import json
from avro import schema
import avro.schema
#from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import re

# function to print all the hashtags in a text
def detect_os(text):

    try:
        # the regular expression
        regex_a = "Android"
        regex_i = "iPhone"

        # extracting the hashtags
        hashtag_list_a = re.findall(regex_a, text)
        hashtag_list_i = re.findall(regex_i, text)

        # printing the hashtag_list
        #print("The hashtags in \"" + text + "\" are :")
        if  hashtag_list_a:
            return "Android"
        elif hashtag_list_i:
            return "iPhone"
        else :
            return "Other"
    except:
        return "Other"


def hash_func(self):
    return hash(str(self))

schema.RecordSchema.__hash__ = hash_func


value_schema = avro.schema.Parse(open("value.avsc").read())

key_schema = avro.schema.Parse(open("key.avsc").read())

def delivery_report(err, msg):

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        #print("")

def buildTestSet(api_kind,search_keyword,search_count,since_id):#, p_geocode = None
    try:

        twitter_api = twitter.Api(consumer_key='',
                        consumer_secret='',
                        access_token_key='',
                        access_token_secret='',
                        tweet_mode= 'extended',
                        sleep_on_rate_limit= True        )
        if api_kind =='GetSearch':
            tweets_fetched = twitter_api.GetSearch(search_keyword,count=search_count ,lang ='es',since_id=since_id)         #,geocode= p_geocode

            return [ status._json for status in tweets_fetched]
        elif api_kind =='GetStatus':
            return twitter_api.GetStatus(status_id=search_keyword)
    except:
        logging.error(str(traceback.format_exc()))
        return None
def transform_date_tw(   input_str   ):
    try:
        input_str1 = str(input_str)[4:19]
        input_str2 = str(input_str)[26:30]
        input_str= input_str1+' '+input_str2
        datetime_object = datetime.strptime(input_str, '%b %d %H:%M:%S %Y')
        #datetime_object = transform_date_docker(datetime_object)
        return datetime_object.strftime('%Y-%m-%d %H:%M:%S')
    except :
            return None

def get_twitter(   l_twitter,s_since_id   ):

    cnt_tw = 0
    lst_dict_twitter=[]

    try:
        for k_search in l_twitter:

            testDataSet = buildTestSet(api_kind='GetSearch',search_keyword =k_search,search_count=180,since_id=s_since_id)

            if testDataSet:

                lst_dict_twitter=[]
                for a in testDataSet:
                    user_date=transform_date_tw(a.get('user',{}).get('created_at',{}))
                    date_date_f = datetime.strptime(user_date, '%Y-%m-%d %H:%M:%S')
                    user_years_ago=datetime.now() -date_date_f
                    user_years_ago=str(int(int(user_years_ago.days)/365)+1)

                    new_json={
                        "source":a.get('source',{}),
                        "full_text":a.get('full_text',{}),
                        "user_name":a.get('user',{}).get('name',{}),
                        "user_followers":str(a.get('user',{}).get('followers_count',{})),
                        "user_friends":str(a.get('user',{}).get('friends_count',{})),
                        "user_created_at":user_date,
                        "user_years_ago":user_years_ago,
                        "user_os": detect_os(a.get('source',{})),
                        "id_str": a.get('id_str',{})
                        }


                    lst_dict_twitter.append(new_json)

        logging.info("Records Loaded : "+str(cnt_tw))
        return lst_dict_twitter

    except:
        logging.error(str(traceback.format_exc()))
        return None

avroProducer = AvroProducer({
    'bootstrap.servers': 'broker:9092',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://schema-registry:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

########################## Twitter Load  #####################
try:
    inicio = datetime.now()
    logging.info('Starting  getting data from Twitter : '+str(datetime.now() -inicio)+' * At : '+ datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    inicio = datetime.now()
    uniq_tweets_l=[]
    df_wsp = pd.read_csv(r"/home/jovyan/work/idstr.csv",sep = ',',encoding='utf-8',quotechar='"')
    last_since_id = str(df_wsp['idstr'][0])

    while True:
        l_twitter =['#covid']
        cnt_tw =  get_twitter(l_twitter,last_since_id)
        #cnt_tw =  get_twitter(l_twitter,'0000000000000000000') # REMOVER

        for data in cnt_tw:
            if data.get('id_str') not in uniq_tweets_l:
                print(data)
                values = {"full_text": data.get('full_text'),"id_str":data.get('id_str'),"user_created_at":data.get('user_created_at'),
                         "source": data.get('source'),"user_name":data.get('user_name'),"user_followers":data.get('user_followers'),
                         "user_years_ago": data.get('user_years_ago'),"user_os":data.get('user_os'),"user_friends":data.get('user_friends')}
                keys = {"id_str": data.get('id_str')}

                avroProducer.produce(topic='coedata_topic', value=values, key=keys)
                uniq_tweets_l.append(data.get('id_str'))
                last_since_id=data.get('id_str')
                d_data =[{'idstr':str(last_since_id)}]
                #get a checkpoint for producer executions
                dfa_tw=pd.DataFrame(data=d_data)
                dfa_tw.to_csv('idstr.csv', index = False, sep = ',', na_rep  = '',quoting=csv.QUOTE_ALL,encoding = 'utf-8')

        avroProducer.flush()
        time.sleep(6)
    logging.info('Ending  getting data from Twitter : '+str(datetime.now() -inicio)+' * At : '+ datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    inicio = datetime.now()

except:
    logging.error(str(traceback.format_exc()))






