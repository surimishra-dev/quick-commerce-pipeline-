import mysql.connector
import pandas as pd

mysql_config = {
    'host': '34.131.10.133',
    'user': 'surimishra',
    'password': 'Surimishra@gmail.com',
    'database': 'mysql',
    'port': 3306
}

def fetch_orders(mysql_config):
    conn = mysql.connector.connect(**mysql_config)
    print=("DB Connected")
    query = """SELECT *
    FROM orders"""
    #WHERE order_ts > DATE_SUB(NOW(), INTERVAL 24 HOUR);"""
    print("Query executing")
    df_orders = pd.read_sql(query, conn)
    print("Data read")
    conn.close()
    print('length is : ', len(df_orders))
    print('result is : ', df_orders)
    return df_orders
#if __name__ == "__main__":
fetch_orders(mysql_config)
