import mysql.connector
import pandas as pd

mysql_config = {
    'host': '34.47.253.108',
    'user': 'abhranil',
    'password': 'Abhranil@89',
    'database': 'QuickCommerce',
    'port': 3306
}

def fetch_orders(mysql_config):
    conn = mysql.connector.connect(**mysql_config)
    query = """SELECT *
    FROM orders"""
    #WHERE order_ts > DATE_SUB(NOW(), INTERVAL 24 HOUR);"""
    df_orders = pd.read_sql(query, conn)
    conn.close()
    print('length is : ', len(df_orders))
    print('result is : ', df_orders)
    return df_orders
#if __name__ == "__main__":
fetch_orders(mysql_config)
