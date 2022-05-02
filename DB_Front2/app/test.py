import mysql.connector

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
sql_string = "SELECT * FROM user_data;"
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

print(result)
        