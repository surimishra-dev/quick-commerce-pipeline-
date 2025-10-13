import mysql.connector as mysql
import MySQLdb
import mysql.connector
import pandas as pd

db = mysql.connect = {
    'host': '34.180.3.122',
    'user': 'abhranil',
    'password': 'Abhranil@89',
    'database': 'QuickCommerce',
    'port': 22
}

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
cur.execute("SELECT * Orders")
# print all the first cell of all the rows
for row in cur.fetchall():
    print(row[0])

db.close()
