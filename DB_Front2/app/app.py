##Imports

from flask import Flask,render_template, request, jsonify
import mysql.connector
import pymongo
from pymongo import MongoClient
import datetime
from datetime import date, timedelta
import time
import redis
import re
import json
import pandas as pd
import numpy as np
from collections import defaultdict

#Define the App
app = Flask(__name__)

#MySQL Connection 

config = {
        'user': 'tomlevy',
        'password': 'TwitteR20$!',
        'host': 'localhost',
        'database': 'twitter2',
        'raise_on_warnings': True,
        'auth_plugin': 'mysql_native_password'
    }

conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary = True)

#Mongo Connection

user = 'tal139'
password = 'porro24scp'
client = MongoClient("mongodb+srv://tal139:porro24scp@cluster0.gyfph.mongodb.net/twitterdb_mongo?retryWrites=true&w=majority")
twitterdb_mongo = client['twitterdb_mongo']
twitter_col = twitterdb_mongo['twitter_col']
pymongo_cursor = twitter_col.find({})
pymongo_tweets = [p for p in pymongo_cursor]

#Redis Connection

redis_client = redis.Redis(host='localhost', port='6379', decode_responses=True)

##Time Functions

def db_timeconvert(a):
    if a.split(' ')[1] == 'Jan':
        m = 1
    if a.split(' ')[1] == 'Feb':
        m = 2
    if a.split(' ')[1] == 'Mar':
        m = 3    
    if a.split(' ')[1] == 'Apr':
        m = 4
    if a.split(' ')[1] == 'May':
        m = 5
    if a.split(' ')[1] == 'Jun':
        m = 6  
    if a.split(' ')[1] == 'Jul':
        m = 7
    if a.split(' ')[1] == 'Aug':
        m = 8
    if a.split(' ')[1] == 'Sep':
        m = 9  
    if a.split(' ')[1] == 'Oct':
        m = 10
    if a.split(' ')[1] == 'Nov':
        m = 11
    if a.split(' ')[1] == 'Dec':
        m = 12
    y = int(a.split(' ')[4])
    d = int(a.split(' ')[2])
    days = abs(datetime.date(y, m, d) - datetime.date(2002, 1, 1))
    total_seconds = days.days * 24 * 3600
    timestamp = a.split(' ')[3]
    timestamp_hr = int(timestamp.split(':')[0])
    timestamp_min = int(timestamp.split(':')[1])
    timestamp_sec = int(timestamp.split(':')[2])
    total_seconds = total_seconds + (timestamp_hr * 3600) + (timestamp_min * 60) + timestamp_sec
    return total_seconds

def search_timeconvert(b):
    m = int(b.split('-')[0])
    d = int(b.split('-')[1])
    y = int(b.split('-')[2])
    days = abs(datetime.date(y, m, d) - datetime.date(2002, 1, 1))
    total_seconds = days.days * 24 * 3600
    return total_seconds

#MySQL + Mongo Overlap

def mysql_mongo_combine():
    sql_string = """SELECT * FROM user_data;"""
    cursor.execute(sql_string)
    rows = cursor.fetchall()
    result = {}
    counter = 0
    for row in rows:
        nested_dict = {}
        for k,v in row.items():
            nested_dict[k] = v.decode()
        filtered_pymongo_tweet = pymongo_tweets[int(nested_dict['user_index'])]
        for l, w in filtered_pymongo_tweet.items():
            if l != 'hashtags':
                nested_dict[l] = w
        result[counter] = nested_dict
        counter += 1
    for i in result:
        if result[i]['retweet_count'] == '':
            result[i]['retweet_count'] = 0
        else:
            result[i]['retweet_count'] = int(result[i]['retweet_count'])
    for i in result:
        result[i]['hashtags'] = result[i]['hashtags'].strip('][').split(',')
    result_list = sorted([result[i] for i in result], key=lambda x: x['retweet_count'], reverse=True)
    return result_list

#Filter Results for Top 10 Tweets
filtered_result_list = mysql_mongo_combine()
users = [i['username'] for i in filtered_result_list]
tweets = [i['tweet_text'] for i in filtered_result_list]
retweet_counts = [i['retweet_count'] for i in filtered_result_list]
top10_df = pd.DataFrame({'user': users, 'tweets':tweets, 'retweet_count':retweet_counts})
top10_df2 = top10_df.loc[(top10_df['user'] != '') &  (top10_df['user'] != ' ')]
top10_df3 = top10_df2.sort_values(by='retweet_count', ascending=False)
top10_df4 = top10_df3.iloc[0:10,:]

#Filter Results for Top 10 Unique Users Through Dictionary + Create Dataframe
top10user_dict = {}
for i in filtered_result_list:
    if i['username'] in top10user_dict.keys():
        top10user_dict[i['username']] += i['retweet_count']
    else:
        top10user_dict[i['username']] = i['retweet_count']

top10user_df = pd.DataFrame({'user':[k for k in top10user_dict.keys()], 'retweet_count':[v for v in top10user_dict.values()]})
top10user_df2 = top10user_df.loc[(top10user_df['user'] != '') &  (top10user_df['user'] != ' ')]
top10user_df3 = top10user_df2.sort_values(by='retweet_count', ascending=False)
top10user_df4 = top10user_df3.iloc[0:10,:]

#Route for Home Page
@app.route("/")
def home():
    return render_template("search.html", template_folder='templates')

#General Search Method
@app.route("/search", methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        #Get Form Names + Define Redis Key
        u_name = request.form['searchtext']
        u_searchtype = request.form.get('searchtype')
        u_starttime = request.form['starttime']
        u_endtime = request.form['endtime']
        print(u_name)
        print(u_searchtype)
        print(u_starttime)
        print(u_endtime)
        redis_key = """{}:{}, {}:{}, {}:{}, {}:{}""".format(1, u_name, 2, u_searchtype, 3, u_starttime, 4, u_endtime)
        
        #Redis Key + Start Time Saved For Conditional Statement
        summary1 = ""
        start_time = time.time()

        #If the key is already in the client, append the results to the summary, measure the time, clean results for HTML
        if (redis_client.exists(redis_key) > 0) and (redis_client.ttl(redis_key) > 0):
            summary1 += str(redis_client.mget(redis_key))
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time_ms = str(round(elapsed_time * 1000)) + 'ms'
            data2 = [i for i in list(eval(summary1))]
            data3 = [str(i) + '}' if (len(data2[0].lstrip('[').rstrip(']').split("}, ")) > 1) else str(i) for i in data2[0].lstrip('[').rstrip(']').split("}, ")]
            print(data3)
            print(elapsed_time_ms)
            return render_template("search.html", template_folder='templates', tables=[pd.DataFrame(data3[:10]).to_html(classes='data')], titles=pd.DataFrame(data3).columns.values)
            #data2 = summary1.strip('][')
        #Otherwise filter on form values
        else:    
            if u_name == '':
                data1 = filtered_result_list
                #return render_template("search.html", template_folder='templates', data=filtered_result_list[:10])
            else:
                if u_searchtype == 'searchtype':
                    data1 = filtered_result_list
                    #return render_template("search.html", template_folder='templates', data=filtered_result_list[:10])
                elif u_searchtype == 'username':
                    data1 = [i for i in filtered_result_list if i['username'] == u_name]
                elif u_searchtype == 'hashtagname':
                    data1 = [i for i in filtered_result_list if u_name in i['hashtags']]
                else:
                    data1 = [i for i in filtered_result_list if u_name in i['tweet_text']]
            #Time Filter
            if (u_starttime != '') & (u_endtime != ''):
                data2 = [i for i in data1 if (search_timeconvert(u_starttime) <=  db_timeconvert(i['tweet_time']) <= search_timeconvert(u_endtime))]
            if (u_starttime == '') & (u_endtime != ''):
                data2 = [i for i in data1 if (db_timeconvert(i['tweet_time']) <= search_timeconvert(u_endtime))]
            if (u_starttime != '') & (u_endtime == ''):
                data2 = [i for i in data1 if (db_timeconvert(i['tweet_time']) >= search_timeconvert(u_starttime))]
            if (u_starttime == '') & (u_endtime == ''):
                data2 = data1
            #Add new search into the client, save it for 10 minutes, record the end time and the elapsed time
            redis_client.setex(redis_key, time=timedelta(minutes=10), value=str(data2))
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time_ms = str(round(elapsed_time * 1000)) + 'ms'
            print(elapsed_time_ms)
            #Render template in flask through Jinja2
            return render_template("search.html", template_folder='templates', tables=[pd.DataFrame(data2[:10]).to_html(classes='data')], titles=pd.DataFrame(data2[:10]).columns.values)
            
#Top 10 Tweets Route
@app.route("/metrics", methods=['GET', 'POST'])
def metrics():
    if request.method == 'POST':
        return render_template("search.html", tables=[top10_df4.to_html(classes='data')], titles=top10_df4.columns.values)

#Top 10 Users Route
@app.route("/metrics2", methods=['GET', 'POST'])
def metrics2():
    if request.method == 'POST':
        return render_template("search.html", tables=[top10user_df4.to_html(classes='data')], titles=top10user_df4.columns.values)

#User Dropdown Attempt
#@app.route("/usernameclick", methods=['GET', 'POST'])
#def usernameclick():
#    if request.method == 'POST':
#        return render_template("search.html", tables=[top10user_df4.to_html(classes='data')], titles=top10user_df4.columns.values)
# @app.route("/hashtagsearch", methods=['GET', 'POST'])


if __name__ == "__main__":
    app.run(debug=True)