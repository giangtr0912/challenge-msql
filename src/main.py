#!/usr/bin/env python3

import os
import sys

sys.path.append(os.getcwd())

# import python modules
import mysql.connector
from tabulate import tabulate

# import methods
from src import queries, utils


class DBHelper():

    def __init__(self):
        # connect with mysql, create database and tables
        utils.db_setup_init(queries)

    def db_insert(self, query, config):
        # query/select/insert data from database
        utils.db_write(query, config)

    def db_fetchall(self, query, db_config, headers):
        # Fetch All and show data
        # https://linuxhint.com/cursor-execute-python/
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        cursor.execute(query)
        print(f"query: \n{cursor.statement}\n result:")
        if cursor.with_rows:
            print(tabulate(cursor.fetchall(), headers=headers,
                           tablefmt='psql'))
        else:
            print("No result\n")

        cursor.close()
        cnx.close()


# main coding
helper = DBHelper()

# Dumping data for `order_items` & `late_fee` table
# import pdb; pdb.set_trace()
for table_name, _query in queries.query_dict_init.items():
    print(f"Begin insert data into {table_name}")
    helper.db_insert(_query, utils.DB_config['staging'])
    print(f"Successfully insert data into {table_name}")

# Answer for Business questions 1
date_time = ["day", "month", "quarter", "year"]
i = 0
for _name, _query in queries.query_dict_bq1.items():
    print(f"\n{_name}")
    headers = ['item_name', 'quantity', f'{date_time[i]}']
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)
    i += 1

# Answer for Business questions 2
for _name, _query in queries.query_dict_bq2.items():
    print(f"\n{_name}")
    headers = ['item_name', 'late_fee_total_amount', 'currency']
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)

# Answer for Business questions 3
date_time = ["day", "month", "quarter", "year"]
i = 0
for _name, _query in queries.query_dict_bq3.items():
    print(f"\n{_name}")
    headers = [
        'merchant_id', 'merchant_name', 'new_order_day', f'{date_time[i]}'
    ]
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)
    i += 1

# Answer for Business questions 4
date_time = ["day", "month", "quarter", "year"]
i = 0
for _name, _query in queries.query_dict_bq4.items():
    print(f"\n{_name}")
    headers = ['merchant_name', 'canceled_order_total', f'{date_time[i]}']
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)
    i += 1

# Answer for Business questions 5
date_time = ["day", "month", "quarter", "year"]
i = 0
for _name, _query in queries.query_dict_bq1.items():
    print(f"\n{_name}")
    headers = ['late_fee_total_amount', 'currency', f'{date_time[i]}']
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)
    i += 1
