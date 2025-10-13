import mysql.connector
import pandas as pd

# Connect to MySQL
db = mysql.connector.connect(
    host="34.180.3.122",
    user="abhranil",
    password="Abhranil@89",
    database="QuickCommerce",
    port=3306  # MySQL default port, not 22 (22 is SSH)
)

# Create a cursor
cur = db.cursor()

# Run your query
cur.execute("SELECT * FROM Orders")

# Fetch all rows
rows = cur.fetchall()
for row in rows:
    print(row)

db.close()
