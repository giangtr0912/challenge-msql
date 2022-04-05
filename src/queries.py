from src import utils

# SQL query for creating staging database IF NOT EXISTS
create_staging_db = ('''CREATE DATABASE IF NOT EXISTS {}'''.format(
    utils.STAGING_DB))

# SQL query for creating production database IF NOT EXISTS
create_production_db = ('''CREATE DATABASE IF NOT EXISTS  {}'''.format(
    utils.PRODUCTION_DB))

create_test_db = ('''CREATE DATABASE IF NOT EXISTS {}'''.format(utils.TEST_DB))

# SQL query for deleting table(s) IF EXISTS
delete_order_items_table = ('''DROP TABLE IF EXISTS {}.{}'''.format(
    utils.STAGING_DB, utils.TABLE_NAME_1))
delete_late_fee_table = ('''DROP TABLE IF EXISTS {}.{}'''.format(
    utils.STAGING_DB, utils.TABLE_NAME_2))
delete_merchant_table = ('''DROP TABLE IF EXISTS {}.{}'''.format(
    utils.STAGING_DB, utils.TABLE_NAME_3))

# SQL query for creating table(s) IF NOT EXISTS
create_order_items_table = ('''
CREATE TABLE {}.{} (
      order_id VARCHAR(36) NOT NULL,
      item_name VARCHAR(255) NULL,
      quantity INT NULL,
      status VARCHAR(20) NULL,
      created_at DATETIME NULL,
      event_name VARCHAR(100) NULL)
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1))

create_late_fee_table = ('''
CREATE TABLE {}.{} (
      order_id VARCHAR(36) NOT NULL,
      payment_id VARCHAR(36) NULL,
      amount FLOAT NULL,
      currency VARCHAR(3) NULL,
      recorded_at DATETIME NULL)
      '''.format(utils.STAGING_DB, utils.TABLE_NAME_2))

create_merchant_table = ('''
CREATE TABLE {}.{} (
      order_id VARCHAR(36) NOT NULL,
      merchant_id VARCHAR(36) NULL,
      merchant_name VARCHAR(100) NULL)
      '''.format(utils.STAGING_DB, utils.TABLE_NAME_3))

# SQL query Dumping data for the table
insert_data_into_order_items_tbl = ('''
    INSERT INTO {}.{} (order_id, item_name, quantity, status, created_at, event_name)
    SELECT order_items_table.*, order_events_table.created_at, order_events_table.event_name
    FROM (
            SELECT JSON_UNQUOTE(payload) AS payload, created_at, CAST(event_name  AS CHAR(100)) AS event_name
            FROM {}.{}) AS order_events_table,
    JSON_TABLE
            (
                JSON_UNQUOTE(payload), '$' COLUMNS
                    (
                        order_id VARCHAR(36) PATH '$.order_id',
                        NESTED PATH '$.items[*]'
                        COLUMNS (
                                    item_name VARCHAR(255) PATH '$.name',
                                    quantity int PATH '$.quantity'
                                ),
                        status VARCHAR(20) PATH '$.status'
                    )
            ) AS order_items_table
    WHERE payload LIKE '%item%'
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.SOURCE_DB,
               utils.SOURCE_TABLE))

insert_data_into_late_fee_tbl = ('''
    INSERT INTO {}.{} (order_id, payment_id, amount, currency, recorded_at)
    SELECT
        CAST(REPLACE(JSON_EXTRACT(JSON_UNQUOTE(payload), '$.order_id'), '"', '') AS CHAR(36)) AS order_id,
        CAST(REPLACE(JSON_EXTRACT(JSON_UNQUOTE(payload), '$.payment_id'), '"', '')AS CHAR(36)) AS payment_id,
        CAST(JSON_EXTRACT(JSON_UNQUOTE(payload), '$.late_fee_amount.amount')AS FLOAT) AS late_fee_amount,
        CAST(REPLACE(JSON_EXTRACT(JSON_UNQUOTE(payload), '$.late_fee_amount.currency'), '"', '') AS CHAR(3)) AS late_fee_currency,
        CAST(JSON_EXTRACT(JSON_UNQUOTE(payload), '$.recorded_at') AS DATETIME) AS recorded_at
    FROM {}.{}
    WHERE payload LIKE '%late%fee%amount%'
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_2, utils.SOURCE_DB,
               utils.SOURCE_TABLE))

insert_data_into_merchant_tbl = ('''
    INSERT INTO {}.{} (order_id, merchant_id, merchant_name)
    SELECT DISTINCT(order_id), t1.*
    FROM {}.{}, JSON_TABLE
            (
                JSON_UNQUOTE(payload), '$' COLUMNS
                    (
                        merchant_id VARCHAR(36) PATH '$.merchant_id',
                        merchant_name VARCHAR(255) PATH '$.merchant_name'
                    )
            ) AS t1
            WHERE t1.merchant_name IS NOT NULL AND t1.merchant_id IS NOT NULL
'''.format(utils.STAGING_DB, utils.TABLE_NAME_3, utils.SOURCE_DB,
           utils.SOURCE_TABLE))

# 1. SQL query to answer for Business questions 1
# 1.1 Top 10 most purchased items by day
b1_1 = ('''
    SELECT item_name, SUM(quantity)  AS quantity, DATE(created_at) AS date
    FROM {}.{}
    WHERE status = '%new%'
    GROUP BY item_name, DATE(created_at)
    ORDER BY SUM(quantity) DESC
    LIMIT 10
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1))

# 1.2 Top 10 most purchased items by month
b1_2 = ('''
    SELECT item_name, SUM(quantity)  AS quantity, MONTH(created_at) AS month
    FROM {}.{}
    WHERE status = '%new%'
    GROUP BY item_name, MONTH(created_at)
    ORDER BY SUM(quantity) DESC
    limit 10
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1))

# 1.3 Top 10 most purchased items by quarter
b1_3 = ('''
    SELECT item_name, SUM(quantity) AS quantity, QUARTER(created_at) AS quarter
    FROM {}.{}
    WHERE status = '%new%'
    GROUP BY item_name, QUARTER(created_at)
    ORDER BY SUM(quantity) DESC
    limit 10
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1))

# 1.4 Top 10 most purchased items by year
b1_4 = ('''
    SELECT item_name, SUM(quantity)  AS quantity, YEAR(created_at) AS year
    FROM {}.{}
    WHERE status = '%new%'
    GROUP BY item_name, YEAR(created_at)
    ORDER BY SUM(quantity) DESC
    limit 10
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1))

# 2. Top 10 items that contributed most to the late fee.
b2_1 = ('''
    SELECT item_name, SUM(late_fee_amount) AS late_fee_total_mount, currency
    FROM (
            SELECT t1.item_name, t2.amount AS late_fee_amount, t2.recorded_at, t2.currency
            FROM {}.{} t1
            JOIN {}.{} t2
            ON t1.order_id = t2.order_id
        ) AS t
    GROUP BY item_name, currency
    ORDER BY SUM(late_fee_amount) DESC
    LIMIT 10
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
               utils.TABLE_NAME_2))

# 3. SQL query to answer for Business questions 3
# 3.1 Top 10 merchants who have most new order value by day
b3_1 = ('''
    SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, DATE(created_at) AS date
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, DATE(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 3.2 Top 10 merchants who have most new order value by month
b3_2 = ('''
    SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, MONTH(created_at) AS month
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, MONTH(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 3.3 Top 10 merchants who have most new order value by quarter
b3_3 = ('''
    SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, QUARTER(created_at) AS quarter
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, QUARTER(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 3.4 Top 10 merchants who have most new order value by year
b3_4 = ('''
    SELECT merchant_id, merchant_name, COUNT(event_name) as new_order_total, YEAR(created_at) AS year
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, YEAR(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 4. SQL query to answer for Business questions 4
# 4.1 Top 10 merchants who have most canceled order value by day
b4_1 = ('''
    SELECT merchant_name, COUNT(event_name) AS canceled_order_total, DATE(created_at) AS date
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, DATE(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 4.2 Top 10 merchants who have most canceled order value by month
b4_2 = ('''
    SELECT merchant_name, COUNT(event_name) AS canceled_order_total, MONTH(created_at) AS month
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, MONTH(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 4.3 Top 10 merchants who have most canceled order value by quarter
b4_3 = ('''
    SELECT merchant_name, COUNT(event_name) AS canceled_order_total, QUARTER(created_at) AS quarter
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, QUARTER(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 4.4 Top 10 merchants who have most canceled order value by year
b4_4 = ('''
    SELECT merchant_name, COUNT(event_name) AS canceled_order_total, YEAR(created_at) AS year
    FROM
    (SELECT * FROM {}.{} t1 NATURAL JOIN {}.{} t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, YEAR(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10'''.format(utils.STAGING_DB, utils.TABLE_NAME_1, utils.STAGING_DB,
                       utils.TABLE_NAME_3))

# 5. SQL query to answer for Business questions 5
# 5.1 Total late fee amount collected by day
b5_1 = ('''
    SELECT SUM(amount) AS late_fee_total_amount, currency, DATE(recorded_at) as date
    FROM {}.{}
    GROUP BY DATE(recorded_at), currency
    ORDER BY DATE(recorded_at)
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_2))

# 5.2 Total late fee amount collected by month
b5_2 = ('''
    SELECT SUM(amount) AS late_fee_total_amount, currency, MONTH(recorded_at) as month
    FROM {}.{}
    GROUP BY MONTH(recorded_at), currency
    ORDER BY MONTH(recorded_at)
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_2))

# 5.3 Total late fee amount collected by quarter
b5_3 = ('''
    SELECT SUM(amount) AS late_fee_total_amount, currency, QUARTER(recorded_at) as quarter
    FROM {}.{}
    GROUP BY QUARTER(recorded_at), currency
    ORDER BY QUARTER(recorded_at)
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_2))

# 5.4 Total late fee amount collected by year
b5_4 = ('''
    SELECT SUM(amount) AS late_fee_total_amount, currency, YEAR(recorded_at) as year
    FROM {}.{}
    GROUP BY YEAR(recorded_at), currency
    ORDER BY YEAR(recorded_at)
    '''.format(utils.STAGING_DB, utils.TABLE_NAME_2))

# INIT
keys_init = [utils.TABLE_NAME_1, utils.TABLE_NAME_2, utils.TABLE_NAME_3]
values_init = [
    insert_data_into_order_items_tbl, insert_data_into_late_fee_tbl,
    insert_data_into_merchant_tbl
]
query_dict_init = dict(zip(keys_init, values_init))

# BQ-01
bq1_keys = [
    "Top 10 most purchased items by day",
    "Top 10 most purchased items by month",
    "Top 10 most purchased items by quarter",
    "Top 10 most purchased items by year"
]
bq1_keys_values = [b1_1, b1_2, b1_3, b1_4]
query_dict_bq1 = dict(zip(bq1_keys, bq1_keys_values))

# BQ-02
bq2_keys = ["Top 10 items that contributed most to the late fee"]
bq2_keys_values = [b2_1]
query_dict_bq2 = dict(zip(bq2_keys, bq2_keys_values))

# BQ-03
bq3_keys = [
    "Top 10 merchants who have most new order value by day",
    "Top 10 merchants who have most new order value by month",
    "Top 10 merchants who have most new order value by quarter",
    "Top 10 merchants who have most new order value by year"
]
bq3_keys_values = [b3_1, b3_2, b3_3, b3_4]
query_dict_bq3 = dict(zip(bq3_keys, bq3_keys_values))

# BQ-04
bq4_keys = [
    "Top 10 merchants who have most canceled order value by day",
    "Top 10 merchants who have most canceled order value by month",
    "Top 10 merchants who have most canceled order value by quarter",
    "Top 10 merchants who have most canceled order value by year"
]
bq4_keys_values = [b4_1, b4_2, b4_3, b4_4]
query_dict_bq4 = dict(zip(bq4_keys, bq4_keys_values))

# BQ-05
bq5_keys = [
    "Total late fee amount collected by day",
    "Total late fee amount collected by month",
    "Total late fee amount collected by quarter",
    "Total late fee amount collected by year"
]
bq5_keys_values = [b5_1, b5_2, b5_3, b5_4]
query_dict_bq1 = dict(zip(bq5_keys, bq5_keys_values))
