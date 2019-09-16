import snowflake.connector as sf
from config import config
import csv
import os

conn = sf.connect(user = config.username, password = config.password, account = config.account)

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql = 'use {}'.format(config.database)
    execute_query(conn, sql)

    sql =  'use warehouse {}'.format(config.wharehouse)
    execute_query(conn, sql)

    try:
        sql = 'alter warehouse {} resume'.format(config.wharehouse)
        execute_query(conn, sql)
    except:
        pass

    sql = 'use schema {}'.format(config.schema)
    execute_query(conn, sql)

    # sql = 'select * from location'
    # cursor = conn.cursor()
    # cursor.execute(sql)

    sql = "Drop table store.store_c.product"
    cursor = conn.cursor()
    cursor.execute(sql)

    sql = "CREATE TABLE IF NOT EXISTS store.store_c.product(product_id number, product_name varchar(500), product_desc varchar(5000), product_category varchar(5000), product_price float, product_image varchar(1000), product_image_id varchar(1000), product_image_bin binary)"
    cursor = conn.cursor()
    cursor.execute(sql)

    store_path = 'product_csv'
    for file in os.listdir(store_path):
        if file.endswith(".csv"):
            filename = (os.path.join(store_path, file))
            print(filename)
            with open(filename, encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    if len(row) == 6:
                        if row[1] == "description":
                            continue
                        print(row)
                        product_id = 0
                        product_name = row[0]
                        product_desc = row[1]
                        product_category = row[2]
                        product_price = row[3].replace('$', '')
                        product_image = row[4]
                        product_image_id = row[5]
                        product_image_bin = 0
                        # print(row[0])
                        # sql = "INSERT INTO store.store_c.location(product_id, product_name, product_desc, product_category, float(product_price), product_image, product_image_id, product_image_bin) VALUES (" + product_id + ", '" + product_name + "', '" + product_desc + "', '" + product_category + "', '" + product_price + "', '" + product_image + "', '" + product_image_id + "', '" + product_image_bin + "')"
                        # print(sql)
                        # cursor = conn.cursor()
                        # cursor.execute(sql)

except Exception as e:
    print(e)
finally:
    conn.close()