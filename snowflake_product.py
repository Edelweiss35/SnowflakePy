import snowflake.connector as sf
from config import config
import csv
import os

conn = sf.connect(user = config.username, password = config.password, account = config.account)
product_id = 0
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

    sql = "CREATE TABLE IF NOT EXISTS store.store_c.product(product_id number, product_name varchar(500), product_desc varchar(5000), product_category varchar(5000), product_price float, product_image varchar(1000), product_image_id varchar(1000), product_image_bin varchar(1000))"
    cursor = conn.cursor()
    cursor.execute(sql)

    product_path = 'product_csv'
    
    for file in os.listdir(product_path):
        if file.endswith(".csv"):
            filename = (os.path.join(product_path, file))
            print(filename)
            with open(filename, encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                product_list = []
                cnt = 0
                for row in csv_reader:
                    if len(row) == 6:
                        if row[1] == "description" or len(row[0]) > 499:
                            continue
                        if "See details in cart" in row[3] or "In-store purchase only" in row[3] or "Click here for price" in row[3] or "Out of stock" in row[3] or "from" in row[3] or len(row[3]) == 0:
                            continue
                        if "PER MONTH" in row[3]:
                            row[3] = row[3].replace(" PER MONTH", "")
                        product_list.append(row)

                    if len(product_list) > 10000:
                        print(product_id)
                        sql_pref = "INSERT INTO store.store_c.product(product_id, product_name, product_desc, product_category, product_price, product_image, product_image_id, product_image_bin) VALUES "
                        limit_cnt = 0
                        for row in product_list:
                            product_name = row[0].replace("'", "*")
                            product_desc = row[1].replace("'", "*")
                            product_name = product_name.replace("\\", "")
                            product_desc = product_desc.replace("\\", "")
                            product_category = row[2].replace("'", "*")
                            product_price = row[3].replace('$', '')
                            product_image = row[4].replace("'", "*")
                            product_image_id = row[5]
                            product_image_bin = "images/" + row[2].replace("'", "*") + "/" + row[5]
                            
                            sql_back = "({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')".format(str(product_id), product_name, product_desc, product_category, product_price, product_image, product_image_id, product_image_bin)
                            
                            if limit_cnt == 0:
                                sql_pref = sql_pref + sql_back
                                product_id = product_id + 1
                                limit_cnt = limit_cnt + 1
                                continue
                            sql_pref = sql_pref + "," + sql_back
                            product_id = product_id + 1
                            
                        cursor = conn.cursor()
                        cursor.execute(sql_pref)
                        product_list.clear()
                        continue

                if len(product_list) == 0:
                    break
                sql_pref = "INSERT INTO store.store_c.product(product_id, product_name, product_desc, product_category, product_price, product_image, product_image_id, product_image_bin) VALUES "
                limit_cnt_1 = 0
                print(product_id)
                for row in product_list:
                    product_name = row[0].replace("'", "*")
                    product_desc = row[1].replace("'", "*")
                    product_name = product_name.replace("\\", "")
                    product_desc = product_desc.replace("\\", "")
                    product_category = row[2].replace("'", "*")
                    product_price = row[3].replace('$', '')
                    product_image = row[4].replace("'", "*")
                    product_image_id = row[5]
                    product_image_bin = "images/" + row[2].replace("'", "*") + "/" + row[5]
                    
                    sql_back = "({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')".format(str(product_id), product_name, product_desc, product_category, product_price, product_image, product_image_id, product_image_bin)
                    
                    if limit_cnt_1 == 0:
                        sql_pref = sql_pref + sql_back
                        product_id = product_id + 1
                        limit_cnt_1 = limit_cnt_1 + 1
                        continue
                    sql_pref = sql_pref + "," + sql_back
                    product_id = product_id + 1
                    
                cursor = conn.cursor()
                cursor.execute(sql_pref)

except Exception as e:
    print(e)
    print(product_id)
finally:
    conn.close()