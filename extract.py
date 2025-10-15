
import pandas as pd

# Connect to MySQL
#db = mysql.connector.connect(
#    host="192.168.224.3",
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


import MySQLdb
import pandas as pd

try:
    conn = MySQLdb.connect(
        host="10.29.208.3",   # replace with your Cloud SQL public IP
        user="suryanshm",
        password="Suri_mishra@12",
        database="quick-commerce",
        port=3306
    )

    print("✅ Connected to Cloud SQL!")

    query = "SELECT * FROM Orders;"
    df = pd.read_sql(query, conn)
    print(df.head())

except MySQLdb.connect.Error as err:
    print(f"❌ Error: {err}")

# finally:
#     if conn.is_connected():
#         conn.close()
#         print("🔒 Connection closed.")

