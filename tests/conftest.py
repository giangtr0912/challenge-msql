import sys

sys.path.append('.../')

import pytest
import textwrap
import mysql.connector

# import methods
from src import utils

MYSQL_DB = utils.DB_config['test']['database']

# SQL query for deleting table(s) IF EXISTS
delete_order_items_table = (
    '''DROP TABLE IF EXISTS tamara_staging.order_items''')
delete_late_fee_table = ('''DROP TABLE IF EXISTS tamara_staging.late_fee''')

# SQL query for creating table(s) IF NOT EXISTS
create_order_items_table = ('''
                CREATE TABLE {}.order_items (
                    order_id VARCHAR(36) NOT NULL,
                    merchant_id VARCHAR(36) NULL,
                    merchant_name VARCHAR(255) NULL,
                    item_name VARCHAR(255) NULL,
                    quantity INT NULL,
                    total_amount FLOAT NULL,
                    currency VARCHAR(3) NULL,
                    status VARCHAR(20) NULL,
                    created_at DATETIME NULL,
                    event_name VARCHAR(100) NULL
                )'''.format(MYSQL_DB))

create_late_fee_table = ('''
CREATE TABLE tamara_staging.late_fee (
      order_id VARCHAR(36) NOT NULL,
      payment_id VARCHAR(36) NULL,
      amount FLOAT NULL,
      currency VARCHAR(3) NULL,
      recorded_at DATETIME NULL)
      ''')


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
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(MYSQL_DB))
        # delete then create tables in staging database: order_items & late_fee
        cursor.execute(
            '''DROP TABLE IF EXISTS {}.order_items'''.format(MYSQL_DB))
        # utils.db_setup_init([query.replace(utils.STAGING_DB, utils.TEST_DB) for query in queries]
        cursor.execute(create_order_items_table)
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
    stmt = textwrap.dedent('''
            INSERT INTO {}.order_items (order_id, merchant_id, merchant_name,
            item_name, quantity, total_amount, currency, status, created_at, event_name)
            SELECT order_items_table.*, CAST(order_events_table.event_name  AS CHAR(100)) AS event_name
            FROM (
                    SELECT event_name, JSON_UNQUOTE(payload) AS payload
                    FROM tamara.order_events) AS order_events_table,
            JSON_TABLE
                    (
                        JSON_UNQUOTE(payload), '$' COLUMNS
                            (
                                order_id VARCHAR(36) PATH '$.order_id',
                                merchant_id VARCHAR(36) PATH '$.merchant_id',
                                merchant_name VARCHAR(255) PATH '$.merchant_name',
                                NESTED PATH '$.items[*]'
                                COLUMNS (
                                            item_name VARCHAR(255) PATH '$.name',
                                            quantity int PATH '$.quantity',
                                            total_amount float PATH '$.total_amount.amount',
                                            currency VARCHAR(3) PATH '$.total_amount.currency'
                                        ),
                                status VARCHAR(20) PATH '$.status',
                                created_at DATETIME PATH '$.created_at'
                            )
                    ) AS order_items_table
            where payload like '%item%';
    '''.format(MYSQL_DB))

    cursor.execute(stmt)
