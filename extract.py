#import mysql.connector
#import pandas as pd

# Connect to MySQL
#db = mysql.connector.connect(
#    host="34.47.253.108",
#    user="abhranil",
#    password="Abhranil@89",
#    database="QuickCommerce",
#    port=3306  # MySQL default port, not 22 (22 is SSH)
#)

# Create a cursor
#cur = db.cursor()

# Run your query
#cur.execute("SELECT * FROM Orders")

# Fetch all rows
#rows = cur.fetchall()
#for row in rows:
#    print(row)

#db.close()


import mysql.connector
import pandas as pd

try:
    conn = mysql.connector.connect(
        host="34.47.253.108",   # replace with your Cloud SQL public IP
        user="abhranil",
        password="Abhranil@89",
        database="QuickCommerce",
        port=3306
    )

    print("✅ Connected to Cloud SQL!")

    query = "SELECT * FROM Orders;"
    df = pd.read_sql(query, conn)
    print(df.head())

except mysql.connector.Error as err:
    print(f"❌ Error: {err}")

finally:
    if conn.is_connected():
        conn.close()
        print("🔒 Connection closed.")

