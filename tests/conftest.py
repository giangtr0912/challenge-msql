import sys

sys.path.append('.../')

import pytest
import textwrap
import mysql.connector

# import methods
from src import utils, queries

MYSQL_DB = utils.DB_config['test']['database']

# SQL query for deleting table(s) IF EXISTS
delete_order_items_table = (
    '''DROP TABLE IF EXISTS tamara_staging.order_items''')
delete_late_fee_table = ('''DROP TABLE IF EXISTS tamara_staging.late_fee''')

# SQL query for creating table(s) IF NOT EXISTS
create_order_items_table = queries.create_order_items_table.replace(
    utils.STAGING_DB, utils.TEST_DB)
create_late_fee_table = queries.create_late_fee_table.replace(
    utils.STAGING_DB, utils.TEST_DB)
create_merchant_table = queries.create_merchant_table.replace(
    utils.STAGING_DB, utils.TEST_DB)

# SQL query for Insert data into table
insert_data_into_order_items_tbl = queries.insert_data_into_order_items_tbl.replace(
    utils.STAGING_DB, utils.TEST_DB)
insert_data_into_late_fee_tbl = queries.insert_data_into_late_fee_tbl.replace(
    utils.STAGING_DB, utils.TEST_DB)
insert_data_into_merchant_tbl = queries.insert_data_into_merchant_tbl.replace(
    utils.STAGING_DB, utils.TEST_DB)


@pytest.fixture(scope='module')
def cnxn():
    cnxn = mysql.connector.connect(**utils.DB_config['init'])
    cursor = cnxn.cursor(dictionary=True)

    # drop database if it already exists
    try:
        cursor.execute("DROP DATABASE {}".format(MYSQL_DB))
        cursor.close()
        print("DB dropped")
    except mysql.connector.Error as err:
        print("{}{}".format(MYSQL_DB, err))

    cursor = cnxn.cursor(dictionary=True)

    # create database and tables
    try:
        cursor.execute(queries.create_test_db)
        cursor.execute(create_order_items_table)
        cursor.execute(create_late_fee_table)
        cursor.execute(create_merchant_table)
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    cnxn.database = MYSQL_DB
    yield cnxn
    cnxn.close()


@pytest.fixture
def cursor(cnxn):
    cursor = cnxn.cursor()
    yield cursor
    cnxn.rollback()


@pytest.fixture
def order_items_project(cursor):
    stmt = textwrap.dedent(insert_data_into_order_items_tbl)
    cursor.execute(stmt)


@pytest.fixture
def late_fee_project(cursor):
    stmt = textwrap.dedent(insert_data_into_late_fee_tbl)
    cursor.execute(stmt)


@pytest.fixture
def merchant_project(cursor):
    stmt = textwrap.dedent(insert_data_into_merchant_tbl)
    cursor.execute(stmt)
