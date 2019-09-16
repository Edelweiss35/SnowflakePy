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

    sql = "Drop table store.store_c.location"
    cursor = conn.cursor()
    cursor.execute(sql)

    sql = "CREATE TABLE IF NOT EXISTS store.store_c.location(location_id number, location_name varchar(500), location_distance varchar(500), location_zip varchar(500), location_city varchar(500), location_phone varchar(500))"
    cursor = conn.cursor()
    cursor.execute(sql)

    store_path = 'store_csv'
    for file in os.listdir(store_path):
        if file.endswith(".csv"):
            filename = (os.path.join(store_path, file))
            with open(filename) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    if len(row) == 7:
                        if row[1] == "store_id":
                            continue
                        print(row[0])
                        sql = "INSERT INTO store.store_c.location(location_id, location_name, location_distance, location_zip, location_city, location_phone) VALUES (" + row[1] + ", '" + row[0] + "', '" + row[2] + "', '" + row[4] + "', '" + row[5] + "', '" + row[6] + "')"
                        print(sql)
                        cursor = conn.cursor()
                        cursor.execute(sql)

except Exception as e:
    print(e)
finally:
    conn.close()