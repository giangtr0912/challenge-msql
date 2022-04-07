#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aim: Tamara Data Engineer Coding Challenge
Run command:
        $ python src/main.py
"""

__author__      = "Giang Tran"
__copyright__   = "Copyright 2022, Tamara"
__credits__ = ["Giang Tran"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Giang Tran"
__email__ = "giangde0912@gmail.com"
__status__ = "Production"

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

    def db_incremental_update_check(self, query, config):
        # query/select/insert data from database
        print(query)
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute(query)
        if cursor.with_rows:
            _temp = [item[0] for item in cursor.fetchall()]
            print(_temp)
            if _temp:
                if _temp[0]:
                    id_max = _temp[0]
                else:
                    id_max = 0
            else:
                id_max = 0
        else:
            id_max = 0

        cursor.close()
        cnx.close()

        return id_max

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

# Dumping data for order_item_main_infos, order_late_fee_infos, and order_merchant_infos table
for table_name, _query in queries.query_dict_init.items():
    print(f"Begin check {table_name} table")
    id_max = helper.db_incremental_update_check(_query[0],
                                                utils.DB_config['staging'])
    print(f"Begin insert data into {table_name} table from id = {id_max}")
    helper.db_insert(_query[1].replace("id > 0", f"id > {id_max}"),
                     utils.DB_config['staging'])
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
    headers = ['merchant_name', 'new_order_value']
    helper.db_fetchall(_query, utils.DB_config['staging'], headers)
    i += 1

# Answer for Business questions 4
date_time = ["day", "month", "quarter", "year"]
i = 0
for _name, _query in queries.query_dict_bq4.items():
    print(f"\n{_name}")
    headers = ['merchant_name', 'canceled_order_total']
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
