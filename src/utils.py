#!/usr/bin/env python3


import sys

import time
import getopt

import MySQLdb
import mysql.connector
from mysql.connector import errorcode

SOURCE_DB = 'tamara'
TEST_DB = 'tamara_test'
STAGING_DB = 'tamara_staging'
PRODUCTION_DB = 'tamara_prod'

SOURCE_TABLE = 'order_events'
TABLE_NAME_1 = 'order_item_main_infos'
TABLE_NAME_2 = 'order_late_fee_infos'
TABLE_NAME_3 = 'order_merchant_infos'


# docker database config init
docker_db_config_init = {
    'user': 'root',
    'password': 'root',
    'host': 'mydb',
    'port': '3306',
    'database': '{}'.format(SOURCE_DB)
}

# database config init
target_db_config_init = {
    'user': 'root',
    'password': 'root',
    'host': 'mydb',
    'port': '3306'
}

# target db
target_db_staging_config = {
    'user': 'root',
    'password': 'root',
    'host': 'mydb',
    'database': '{}'.format(STAGING_DB)
}

# source db
target_db_production_config = {
    'user': 'root',
    'password': 'root',
    'host': 'mydb',
    'database': '{}'.format(PRODUCTION_DB)
}

# database config init
test_db_config = {
    'user': 'root',
    'password': 'root',
    'host': 'mydb',
    'database': '{}'.format(TEST_DB)
}

DB_config = {'docker_init': docker_db_config_init, 'init': target_db_config_init, 'test': test_db_config, 'staging': target_db_staging_config, 'production': target_db_production_config}


def db_setup_init(queries, config=DB_config):
    try:
        # establish source db connection
        cnx = mysql.connector.connect(**config['init'])
        cursor = cnx.cursor()

        # create staging database: tamara_staging
        cursor.execute(queries.create_staging_db)
        cursor.execute(f"USE {config['staging']['database']}")

        # delete then create tables in staging database: order_items & late_fee
        cursor.execute(queries.delete_order_items_table)
        cursor.execute(queries.create_order_items_table)

        cursor.execute(queries.delete_late_fee_table)
        cursor.execute(queries.create_late_fee_table)

        cursor.execute(queries.delete_merchant_table)
        cursor.execute(queries.create_merchant_table)

        # create production database: tamara_prod
        cursor.execute(queries.create_production_db)

        # close the database connection
        cursor.close()
        cnx.close()
        print ("Databases initialization successful")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User authorization error")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)
        return False
    else:
        cnx.close()
        return False
    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Connection closed")


def db_read(query, config, params=None):
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        entries = cursor.fetchall()
        cursor.close()
        cnx.close()

        content = []

        for entry in entries:
            content.append(entry)

        return content

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User authorization error")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)
    else:
        cnx.close()
    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Connection closed")


def db_write(query, config, params=None):
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            cnx.commit()
            cursor.close()
            cnx.close()
            return True

        except MySQLdb._exceptions.IntegrityError:
            cursor.close()
            cnx.close()
            return False

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User authorization error")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)
        return False
    else:
        cnx.close()
        return False
    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Connection closed")
