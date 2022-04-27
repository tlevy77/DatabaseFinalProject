from flask import Flask,render_template, request, jsonify
import mysql.connector

app = Flask(__name__)

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

@app.route("/")
def home():
    return render_template("search.html", template_folder='templates')

@app.route("/usersearch")
def usersearch():
    sql_string = f"SELECT * FROM user_data;"
    cursor.execute(sql_string)
    rows = cursor.fetchall()
    result = {}
    counter = 0

    for row in rows:
        nested_dict = {}
        for k,v in row.items():
            nested_dict[k] = v.decode()
        result[counter] = nested_dict
        counter += 1
    return render_template("search.html", template_folder='templates', data=[result[i] for i in result])

@app.route("/usernamesearch", methods=['GET', 'POST'])
def usernamesearch():
    if request.method == 'POST':
        u_name = request.form['username']
        sql_string = """SELECT * FROM user_data WHERE username = '{}';""".format(u_name)
        cursor.execute(sql_string)
        rows = cursor.fetchall()
        result = {}
        counter = 0

        for row in rows:
            nested_dict = {}
            for k,v in row.items():
                nested_dict[k] = v.decode()
            result[counter] = nested_dict
            counter += 1
        return jsonify(result)

@app.route("/hashtagsearch")
def hashtagsearch():
    #if request.method == 'POST':
    #u_hashtag = request.form['hashtag']
    sql_string = """SELECT * FROM user_data"""
    cursor.execute(sql_string)
    rows = cursor.fetchall()
    init_result = {}
    hashtag_compile = {}
    counter = 0

    for row in rows:
        nested_dict = {}
        for k,v in row.items():
            nested_dict[k] = v.decode()
        init_result[counter] = nested_dict
        counter += 1
    
    for row in init_result:
        hashtag_compile[row] = init_result[row]['hashtags'].strip('][').split(',')

    print(hashtag_compile)
    return hashtag_compile


if __name__ == "__main__":
    app.run(debug=True)