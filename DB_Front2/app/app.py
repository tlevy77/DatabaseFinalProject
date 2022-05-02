from flask import Flask,render_template, request, jsonify
import mysql.connector
import pymongo
from pymongo import MongoClient
import datetime
from datetime import date, timedelta
import time
import redis
import yaml
import json

app = Flask(__name__)

config = {
        'user': 'tomlevy',
        'password': 'TwitteR20$!',
        'host': 'localhost',
        'database': 'twitter2',
        'raise_on_warnings': True,
        'auth_plugin': 'mysql_native_password'
    }

#MySQL Connection 

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

filtered_result_list = mysql_mongo_combine()

@app.route("/")
def home():
    return render_template("search.html", template_folder='templates')

@app.route("/search", methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        u_name = request.form['searchtext']
        u_searchtype = request.form.get('searchtype')
        u_starttime = request.form['starttime']
        u_endtime = request.form['endtime']
        print(u_name)
        print(u_searchtype)
        print(u_starttime)
        print(u_endtime)
        redis_key = """{}:{}, {}:{}, {}:{}, {}:{}""".format(1, u_name, 2, u_searchtype, 3, u_starttime, 4, u_endtime)
        
        summary1 = ""
        start_time = time.time()

        if (redis_client.exists(redis_key) > 0) and (redis_client.ttl(redis_key) > 0):
            msg1 = "Found in redis cache. Generating summary write away"
            summary1 += str(redis_client.mget(redis_key))
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time_ms = str(round(elapsed_time * 1000)) + 'ms'
            msg2 = "Summary generation time:" + elapsed_time_ms
            print(summary1.replace('\\', ''))
            data2 = [i for i in list(eval(summary1))]
            print(data2)
            #data2 = summary1.strip('][')
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

            if (u_starttime != '') & (u_endtime != ''):
                data2 = [i for i in data1 if (search_timeconvert(u_starttime) <=  db_timeconvert(i['tweet_time']) <= search_timeconvert(u_endtime))]
            if (u_starttime == '') & (u_endtime != ''):
                data2 = [i for i in data1 if (db_timeconvert(i['tweet_time']) <= search_timeconvert(u_endtime))]
            if (u_starttime != '') & (u_endtime == ''):
                data2 = [i for i in data1 if (db_timeconvert(i['tweet_time']) >= search_timeconvert(u_starttime))]
            if (u_starttime == '') & (u_endtime == ''):
                data2 = data1
            redis_client.setex(redis_key, time=timedelta(minutes=10), value=str(data2))
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time_ms = str(round(elapsed_time * 1000)) + 'ms'

        print(elapsed_time_ms)
        return render_template("search.html", template_folder='templates', data=data2[:10])

# @app.route("/hashtagsearch", methods=['GET', 'POST'])
# def hashtagsearch():
#     if request.method == 'POST':
#         u_hashtag = request.form['hashtagname']
#         sql_string = """SELECT * FROM user_data"""
#         cursor.execute(sql_string)
#         rows = cursor.fetchall()
#         init_result = {}
#         hashtag_compile = {}
#         hashtagindex_compile = []
#         final_hashtag = {}
#         counter = 0

#         for row in rows:
#             nested_dict = {}
#             for k,v in row.items():
#                 nested_dict[k] = v.decode()
#                 init_result[counter] = nested_dict
#                 counter += 1
#             filtered_pymongo_tweet = pymongo_tweets[int(nested_dict['user_index'])]
#             nested_dict['retweet_count'] = filtered_pymongo_tweet['retweet_count']
#             init_result[counter] = nested_dict
#             counter += 1
    
#         for row in init_result:
#             hashtag_compile[row] = init_result[row]['hashtags'].strip('][').split(',')
        
#         for row in hashtag_compile:
#             if u_hashtag in hashtag_compile[row]:
#                 hashtagindex_compile.append(row)
#             else:
#                 pass

#         for row in init_result:
#             if row in hashtagindex_compile:
#                 final_hashtag[row] = init_result[row]
#             else:
#                 pass

#         double_check = []
        
#         for i in [final_hashtag[i] for i in final_hashtag]:
#             if i not in double_check:
#                 double_check.append(i)

#         for i in double_check:
#             if i['retweet_count'] == '':
#                 i['retweet_count'] = 0
#             else:
#                 i['retweet_count'] = int(i['retweet_count'])
        
#         result_list = sorted(double_check, key=lambda x: x['retweet_count'], reverse=True)

#         print(sorted(double_check, key=lambda x: x['retweet_count']))
#         return render_template("search.html", template_folder='templates', data=result_list[:10])

# @app.route("/textsearch", methods=['GET', 'POST'])
# def textsearch():
    
#     if request.method == 'POST':
#         u_text = request.form['textname']
#         text_starttime = request.form['starttime']
#         text_endtime = request.form['endtime']
#         print(u_text)
#         print(text_starttime)
#         print(text_endtime)
#         #text_starttime = search_timeconvert(request.form.get('starttime'))
#         #text_endtime = search_timeconvert(request.form.get('endtime'))
#         result = {}
#         for i in pymongo_tweets:
#             if u_text in i['tweet_text']:
#                 result[i['_id']] = i
#         for i in result:
#             if result[i]['retweet_count'] == '':
#                 result[i]['retweet_count'] = 0
#             else:
#                 result[i]['retweet_count'] = int(result[i]['retweet_count'])
#         result_list = sorted([result[i] for i in result], key=lambda x: x['retweet_count'], reverse=True)
#         #time_filtered_list = list(filter(lambda x: (text_starttime <= db_timeconvert(x['tweet_time']) <= text_endtime), result_list))
#         return render_template("search.html", template_folder='templates', data=result_list[:10])
#     # if request.method == 'POST':
#     #     u_text = request.form['textname']
#     #     sql_string = """SELECT * FROM user_data WHERE text LIKE '{}';""".format(u_text)
#     #     cursor.execute(sql_string)
#     #     rows = cursor.fetchall()
#     #     result = {}
#     #     counter = 0

#     #     for row in rows:
#     #         nested_dict = {}
#     #         for k,v in row.items():
#     #             nested_dict[k] = v.decode()
#     #         result[counter] = nested_dict
#     #         counter += 1
#     #     return render_template("search.html", template_folder='templates', data=[result[i] for i in result])
# # @app.route("/hashtagsearch", methods=['GET', 'POST'])
# # def hashtagsearch():
# #     if request.method == 'POST':
# #         u_hashtag = request.form['hashtag']
# #         sql_string = """SELECT * FROM user_data WHERE username = '{}';""".format(u_hashtag)
# #         cursor.execute(sql_string)
# #         rows = cursor.fetchall()
# #         init_result = {}
# #         hashtag_compile = {}
# #         counter = 0

# #         for row in rows:
# #             nested_dict = {}
# #             for k,v in row.items():
# #                 nested_dict[k] = v.decode()
# #             init_result[counter] = nested_dict
# #             counter += 1
        
# #         for row in init_result:
# #             hashtag_compile[row] = init_result[row]['hashtags'].strip('][').split(',')

# #         print(hashtag_compile)
# #         return hashtag_compile


if __name__ == "__main__":
    app.run(debug=True)