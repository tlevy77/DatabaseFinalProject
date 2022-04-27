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
    #results = jsonify(result)
    print([result[i] for i in result])
    return render_template("search.html", template_folder='templates', data=result)

@app.route("/usernamesearch", methods=['GET', 'POST'])
def usernamesearch():
    if request.method == 'POST':
        username = request.form['username']  
        sql_string = f"SELECT * FROM user_data WHERE username = {username};"
        #cursor.execute(sql_string)
        #rows = cursor.fetchall()
        #result = {}
        #counter = 0

        #for row in rows:
        #    nested_dict = {}
        #    for k,v in row.items():
        #        nested_dict[k] = v.decode()
        #    result[counter] = nested_dict
        #    counter += 1
        #return jsonify(result)
        print(sql_string)
        return sql_string


if __name__ == "__main__":
    app.run(debug=True)