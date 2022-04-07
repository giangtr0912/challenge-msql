# Tamara Data Engineer Coding Challenge

## Requirements
Our data warehouse system was replicated from our application data (in real time). The payload for order events in our application is in json format which didn't optimize for analytics applications. So when we need query/aggregate fields in a JSON column, it will take a considerable time. And we can't add indexes to fields inside the JSON column to speed up the query.

So we need to create some pipelines to denormalize JSON fields in order_events tables. The new data structure should answer business questions by conducting simple queries:

- Top 10 most purchased items by day, month, quarter, year.
- Top 10 items that contributed most to the late fee.
- Top 10 merchants who have most new order value by day, month, quarter, year
- Top 10 merchants who have most canceled order value by day, month, quarter, year
- Total late fee amount collected by day, month, quarter, year.

---
### Acceptance Criteria
- Setup project structure + Docker + code linting + mypy (Done)
- Storage: (Done)
Input: MySQL
Output: Mysql
- Answer all critical business questions (Done)

### Good to have:
- Pipeline that support incremental update in real time (Done)
- Unit tests and integration tests for critical logic (Done)
- Architecture: Cloud native (K8s) (Under investigation)
- Define CICD (github, gitlab,...) (Underinvestigation)

# Actions & Results
## Explore the data and the proposal of data structure
Order_event table contains information of 4063 states (event_name) of 1000 orders (order_id) from different merchants (merchant_name & merchant_id). In order to answer the given business questions, some parameters need to be archieved from the payload Json field, named: name of item, name of merchant, late fee amount, and time. Based on the following bussiness question, a detailed analyst was conducted:
+ Top 10 most purchased items by day, month, quarter, year:
--> requires name of items (item_name): item iformation is stored in key `item` of payload and store as a list, to reformatad and extract those info, JSON_TABLE() and JSON_UNQUOTE() are used, quantity of items (quantity), time (created_at).
+ Top 10 items that contributed most to the late fee:
--> requires name of items, late fee (event_name) and time.
+ Top 10 merchants who have most new order value by day, month, quarter, year:
--> requires merchant's name, merchant's id, event_name and time.
+ Top 10 merchants who have most canceled order value by day, month, quarter, year:
--> requires merchant's name, merchant's id, canceled status (status, or event_name) and time.
+ Total late fee amount collected by day, month, quarter, year:
--> requires late fee amount, currency and time.


In order to answer the above business questions by simple queries, 3 tables are created:

+ Table 1 named **order_item_main_infos**, it helps to make the simple query to answer for the question number 1. The table stores infos of purchased (id, order_id, item_name, quantity, status, created_at and event_name).
+ Table 2 named **order_late_fee_infos**,  it helps to make the simple query to answer for the question number 2 and 5. The table stores infos of purchased (id, order_id, payment_id, amount, currency and recorded_at).
+ Table 3 named **order_merchant_infos**, it helps to make the simple query to answer for the question number 3 and 4. The table stores infos of purchased (id, order_id, item_name, merchant_id, merchant_name, event_name and created_at).


## Build the App

App is a Python library for dealing with data challenge.

### Prerequisites

Use this link [Docker](https://docs.docker.com/engine/install/) to install docker.

### Project folder structure design
```bash
.
├── docker-compose.yml
├── Dockerfile
├── dumps
│   └── tamara.sql
├── LICENSE
├── output
│   └── tamara_staging.sql
├── README.md
├── requirements.txt
├── run.sh
├── src
│   ├── __init__.py
│   ├── main.py
│   ├── queries.py
│   ├── utils.py
│   └── wait_for_mysql.py
└── tests
    ├── conftest.py
    ├── mock_db.py
    ├── test_database.py
    └── test_utils.py

4 directories, 17 files

```
### Usage
#### Execution command

```bash
sh run.sh
```

#### Execution outcomes
```bash
mydb
mydb
Start building the app
db uses an image, skipping
Building app
Step 1/7 : FROM python:3.8
 ---> 0a91fd9cc482
Step 2/7 : ENV PYTHONBUFFERED 1
 ---> Using cache
 ---> cfcaa40a4e44
Step 3/7 : WORKDIR /tamara
 ---> Using cache
 ---> b5ce127ad562
Step 4/7 : ADD requirements.txt requirements.txt
 ---> Using cache
 ---> aff014bb47f3
Step 5/7 : RUN python3 -m pip install --upgrade pip setuptools && python3 -m pip install -r requirements.txt
 ---> Using cache
 ---> 8bbf925dd69b
Step 6/7 : ADD ./ /tamara
 ---> 9f598c9c477d
Step 7/7 : VOLUME ["/challenge-msql"]
 ---> Running in 27624e58ad44
Removing intermediate container 27624e58ad44
 ---> 1f95a1bacf81
Successfully built 1f95a1bacf81
Successfully tagged app:latest
Running the app
Creating mydb ... done

        



        ################################
        database connect:
        host = db
        user = root
        password = root
        port = 3306
        db = tamara
        ################################ 




MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
MYSQL not responds.. waiting for mysql up: (2002, "Can't connect to MySQL server on 'db' (115)")
database tamara create successful
======================================================================================= test session starts ========================================================================================
platform linux -- Python 3.8.13, pytest-7.1.1, pluggy-1.0.0
rootdir: /tamara
collected 4 items                                                                                                                                                                                  

tests/test_database.py ...                                                                                                                                                                   [ 75%]
tests/test_utils.py .                                                                                                                                                                        [100%]

======================================================================================== 4 passed in 0.95s =========================================================================================
Databases initialization successful
Begin check order_item_main_infos table
SELECT id FROM tamara_staging.order_item_main_infos ORDER BY id DESC LIMIT 1
[]
Begin insert data into order_item_main_infos table from id = 0
Successfully insert data into order_item_main_infos
Begin check order_late_fee_infos table
SELECT MAX(id) FROM tamara_staging.order_late_fee_infos ORDER BY id DESC LIMIT 1
[None]
Begin insert data into order_late_fee_infos table from id = 0
Successfully insert data into order_late_fee_infos
Begin check order_merchant_infos table
SELECT MAX(id) FROM tamara_staging.order_merchant_infos ORDER BY id DESC LIMIT 1
[None]
Begin insert data into order_merchant_infos table from id = 0
Successfully insert data into order_merchant_infos

Total late fee amount collected by day
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, DATE(recorded_at) as date
    FROM tamara_staging.order_late_fee_infos
    GROUP BY DATE(recorded_at), currency
    ORDER BY DATE(recorded_at)
 result:
+-------------+------------+------------+
|   item_name | quantity   | day        |
|-------------+------------+------------|
|     25      | SAR        | 2020-08-07 |
|     25      | SAR        | 2020-08-14 |
|     25      | SAR        | 2020-09-17 |
|     25      | SAR        | 2020-09-24 |
|      8.9375 | SAR        | 2020-10-02 |
|     25      | SAR        | 2020-10-18 |
|     50      | SAR        | 2020-10-25 |
|     25      | SAR        | 2020-10-26 |
|     25      | SAR        | 2020-10-27 |
|     25      | SAR        | 2020-11-02 |
|     25      | SAR        | 2020-11-03 |
|     25      | SAR        | 2020-11-04 |
|     50      | SAR        | 2020-11-10 |
|     75      | SAR        | 2020-11-11 |
|     50      | SAR        | 2020-11-14 |
|     25      | SAR        | 2020-11-15 |
|    150      | SAR        | 2020-11-17 |
|     50      | SAR        | 2020-11-18 |
|     63.6475 | SAR        | 2020-11-19 |
|     75      | SAR        | 2020-11-20 |
|    150      | SAR        | 2020-11-21 |
|    125      | SAR        | 2020-11-22 |
|     25      | SAR        | 2020-11-23 |
|     88.995  | SAR        | 2020-11-25 |
|    100.5    | SAR        | 2020-11-26 |
|     25      | SAR        | 2020-11-27 |
|    225      | SAR        | 2020-11-28 |
|     25      | SAR        | 2020-11-29 |
|     50      | SAR        | 2020-11-30 |
|     50      | SAR        | 2020-12-01 |
|    150      | SAR        | 2020-12-02 |
|     75      | SAR        | 2020-12-03 |
|    175      | SAR        | 2020-12-05 |
|     79.5    | SAR        | 2020-12-06 |
|    125      | SAR        | 2020-12-07 |
|    150      | SAR        | 2020-12-08 |
|    175      | SAR        | 2020-12-09 |
|     67.3125 | SAR        | 2020-12-10 |
|     31.125  | SAR        | 2020-12-12 |
|    136      | SAR        | 2020-12-14 |
|     25      | SAR        | 2020-12-15 |
|     25      | SAR        | 2020-12-16 |
|     25      | SAR        | 2020-12-17 |
|    147.15   | SAR        | 2020-12-18 |
|     25      | SAR        | 2020-12-19 |
|     75      | SAR        | 2020-12-21 |
|     75      | SAR        | 2020-12-22 |
|    150      | SAR        | 2020-12-23 |
|    316.468  | SAR        | 2020-12-24 |
|     25      | SAR        | 2020-12-26 |
|     87.5    | SAR        | 2020-12-29 |
|     25      | SAR        | 2020-12-31 |
|     25      | SAR        | 2021-01-01 |
|     25      | SAR        | 2021-01-02 |
|    101      | SAR        | 2021-01-03 |
|     25      | SAR        | 2021-01-04 |
|     28.1875 | SAR        | 2021-01-05 |
|     50      | SAR        | 2021-01-06 |
|    110      | SAR        | 2021-01-07 |
|    223.805  | SAR        | 2021-01-09 |
|     15.7525 | SAR        | 2021-01-11 |
|     69      | SAR        | 2021-01-14 |
|     25      | SAR        | 2021-01-15 |
|     50      | SAR        | 2021-01-17 |
|     66.8575 | SAR        | 2021-01-18 |
|     25      | SAR        | 2021-01-19 |
|     75      | SAR        | 2021-01-21 |
|     95      | SAR        | 2021-01-22 |
|    174.75   | SAR        | 2021-01-24 |
|     50      | SAR        | 2021-01-29 |
|     50      | SAR        | 2021-02-01 |
|     15.3925 | SAR        | 2021-02-02 |
|     25      | SAR        | 2021-02-03 |
|    143.5    | SAR        | 2021-02-06 |
|    121.167  | SAR        | 2021-02-08 |
|     50      | SAR        | 2021-02-13 |
|     37.5    | SAR        | 2021-02-16 |
|     15      | SAR        | 2021-02-18 |
|     40      | SAR        | 2021-02-21 |
|     54.82   | SAR        | 2021-02-23 |
|     87.375  | SAR        | 2021-03-03 |
|    137.5    | SAR        | 2021-03-04 |
|     25      | AED        | 2021-03-09 |
|     25      | AED        | 2021-03-14 |
|    140      | SAR        | 2021-04-27 |
|     37.6625 | SAR        | 2021-06-16 |
|   3430.7    | SAR        | 2021-08-31 |
+-------------+------------+------------+

Total late fee amount collected by month
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, MONTH(recorded_at) as month
    FROM tamara_staging.order_late_fee_infos
    GROUP BY MONTH(recorded_at), currency
    ORDER BY MONTH(recorded_at)
 result:
+-------------+------------+---------+
|   item_name | quantity   |   month |
|-------------+------------+---------|
|   1234.35   | SAR        |       1 |
|    552.38   | SAR        |       2 |
|     50      | AED        |       3 |
|    224.875  | SAR        |       3 |
|    140      | SAR        |       4 |
|     37.6625 | SAR        |       6 |
|   3480.7    | SAR        |       8 |
|     50      | SAR        |       9 |
|    133.938  | SAR        |      10 |
|   1428.14   | SAR        |      11 |
|   2215.06   | SAR        |      12 |
+-------------+------------+---------+

Total late fee amount collected by quarter
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, QUARTER(recorded_at) as quarter
    FROM tamara_staging.order_late_fee_infos
    GROUP BY QUARTER(recorded_at), currency
    ORDER BY QUARTER(recorded_at)
 result:
+-------------+------------+-----------+
|   item_name | quantity   |   quarter |
|-------------+------------+-----------|
|      50     | AED        |         1 |
|    2011.61  | SAR        |         1 |
|     177.662 | SAR        |         2 |
|    3530.7   | SAR        |         3 |
|    3777.14  | SAR        |         4 |
+-------------+------------+-----------+

Total late fee amount collected by year
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, YEAR(recorded_at) as year
    FROM tamara_staging.order_late_fee_infos
    GROUP BY YEAR(recorded_at), currency
    ORDER BY YEAR(recorded_at)
 result:
+-------------+------------+--------+
|   item_name | quantity   |   year |
|-------------+------------+--------|
|     3877.14 | SAR        |   2020 |
|       50    | AED        |   2021 |
|     5619.97 | SAR        |   2021 |
+-------------+------------+--------+

Top 10 items that contributed most to the late fee
query: 
SELECT item_name, SUM(late_fee_amount) AS late_fee_total_mount, currency
    FROM (
            SELECT t1.item_name, t2.amount AS late_fee_amount, t2.recorded_at, t2.currency
            FROM tamara_staging.order_item_main_infos t1
            JOIN tamara_staging.order_late_fee_infos t2
            ON t1.order_id = t2.order_id
        ) AS t
    GROUP BY item_name, currency
    ORDER BY SUM(late_fee_amount) DESC
    LIMIT 10
 result:
+----------------------------------------------+-------------------------+------------+
| item_name                                    |   late_fee_total_amount | currency   |
|----------------------------------------------+-------------------------+------------|
| رموش ريد شيري                                |                  1200   | SAR        |
| وردة طبيعية مطلية بالذهب "نوعيه فاخرة"       |                  1150   | SAR        |
| جاردن اوليان - المجموعه المغربيه المثاليه  1 |                  1090.5 | SAR        |
| سماعة   AirPods pro من شركة موج مكس          |                   881   | SAR        |
| كوب سيراميك مميز اسود                        |                   700   | SAR        |
| طاحونة يدوية كبيرة                           |                   700   | SAR        |
| ابريق ترشيح 600 مل                           |                   700   | SAR        |
| قلم كحل بيج ساندي بل                         |                   600   | SAR        |
| عطر لانكوم ايدول النسائي او دو بارفيوم 50مل  |                   600   | SAR        |
| عطر باكو رابان أولمبيا انتنس 80ML            |                   600   | SAR        |
+----------------------------------------------+-------------------------+------------+

Top 10 merchants who have most new order value by day
query: 
SELECT merchant_name, COUNT(merchant_id) AS new_order_value
    FROM tamara_staging.order_merchant_infos
    WHERE DATE(created_at) = '2020-10-20'
    GROUP BY merchant_name
    ORDER BY COUNT(merchant_id) DESC
    LIMIT 10
 result:
+-----------------+-------------------+
| merchant_name   |   new_order_value |
|-----------------+-------------------|
| Saramakeup      |                10 |
| Mix store       |                 5 |
| Salla-Test      |                 2 |
| PowerX          |                 2 |
| E-things Store  |                 2 |
| Tanto Shop      |                 1 |
| Dar Lena        |                 1 |
| Elegant Look    |                 1 |
| nadally         |                 1 |
+-----------------+-------------------+

Top 10 merchants who have most new order value by month
query: 
SELECT merchant_name, COUNT(merchant_id) AS new_order_value
    FROM tamara_staging.order_merchant_infos
    WHERE MONTH(created_at) = 10
    GROUP BY merchant_name
    ORDER BY COUNT(merchant_id) DESC
    LIMIT 10
 result:
+-----------------+-------------------+
| merchant_name   |   new_order_value |
|-----------------+-------------------|
| Saramakeup      |                58 |
| Mix store       |                49 |
| PowerX          |                40 |
| Dar Lena        |                27 |
| Glamour         |                24 |
| Automart SA     |                18 |
| Optique         |                18 |
| Bsthebue        |                17 |
| Namshi          |                16 |
| Wosof           |                13 |
+-----------------+-------------------+

Top 10 merchants who have most new order value by quarter
query: 
SELECT merchant_name, COUNT(merchant_id) AS new_order_value
    FROM tamara_staging.order_merchant_infos
    WHERE QUARTER(created_at) = 4
    GROUP BY merchant_name
    ORDER BY COUNT(merchant_id) DESC
    LIMIT 10
 result:
+-----------------+-------------------+
| merchant_name   |   new_order_value |
|-----------------+-------------------|
| Saramakeup      |               114 |
| Dar Lena        |                80 |
| PowerX          |                72 |
| Mix store       |                53 |
| Namshi          |                51 |
| Glamour         |                39 |
| Johrh           |                34 |
| Bsthebue        |                27 |
| Optique         |                22 |
| Automart SA     |                21 |
+-----------------+-------------------+

Top 10 merchants who have most new order value by year
query: 
SELECT merchant_name, COUNT(merchant_id) AS new_order_value
    FROM tamara_staging.order_merchant_infos
    WHERE YEAR(created_at) = 2020
    GROUP BY merchant_name
    ORDER BY COUNT(merchant_id) DESC
    LIMIT 10
 result:
+-----------------+-------------------+
| merchant_name   |   new_order_value |
|-----------------+-------------------|
| Saramakeup      |               133 |
| Dar Lena        |                80 |
| PowerX          |                72 |
| Glamour         |                61 |
| Mix store       |                53 |
| Namshi          |                50 |
| Optique         |                34 |
| Johrh           |                34 |
| Bsthebue        |                27 |
| Elegant Look    |                23 |
+-----------------+-------------------+

Top 10 merchants who have most canceled order value by day
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total
    FROM tamara_staging.order_merchant_infos
    WHERE event_name LIKE '%OrderWasCanceled%' OR event_name LIKE '%OrderWasResolved%' AND DATE(created_at) = '2020-09-30'
    GROUP BY merchant_name
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+
| merchant_name   |   canceled_order_total |
|-----------------+------------------------|
| Saramakeup      |                      3 |
| Fernaz Cafe     |                      2 |
| Namshi          |                      2 |
| Theviolet store |                      2 |
| qatfat          |                      2 |
| Mioist1         |                      1 |
| Salla-Test      |                      1 |
| Elegant Look    |                      1 |
| Dar Lena        |                      1 |
| Coffee drop SA  |                      1 |
+-----------------+------------------------+

Top 10 merchants who have most canceled order value by month
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total
    FROM tamara_staging.order_merchant_infos
    WHERE event_name LIKE '%OrderWasCanceled%' OR event_name LIKE '%OrderWasResolved%' AND MONTH(created_at) = 9
    GROUP BY merchant_name
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+
| merchant_name   |   canceled_order_total |
|-----------------+------------------------|
| Saramakeup      |                      3 |
| Fernaz Cafe     |                      2 |
| Namshi          |                      2 |
| Theviolet store |                      2 |
| qatfat          |                      2 |
| Mioist1         |                      1 |
| Salla-Test      |                      1 |
| Elegant Look    |                      1 |
| Dar Lena        |                      1 |
| Coffee drop SA  |                      1 |
+-----------------+------------------------+

Top 10 merchants who have most canceled order value by quarter
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total
    FROM tamara_staging.order_merchant_infos
    WHERE event_name LIKE '%OrderWasCanceled%' OR event_name LIKE '%OrderWasResolved%' AND QUARTER(created_at) = 3
    GROUP BY merchant_name
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+
| merchant_name   |   canceled_order_total |
|-----------------+------------------------|
| Saramakeup      |                      3 |
| Fernaz Cafe     |                      2 |
| Namshi          |                      2 |
| Theviolet store |                      2 |
| qatfat          |                      2 |
| Mioist1         |                      1 |
| Salla-Test      |                      1 |
| Elegant Look    |                      1 |
| Dar Lena        |                      1 |
| Coffee drop SA  |                      1 |
+-----------------+------------------------+

Top 10 merchants who have most canceled order value by year
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total
    FROM tamara_staging.order_merchant_infos
    WHERE event_name LIKE '%OrderWasCanceled%' OR event_name LIKE '%OrderWasResolved%' AND YEAR(created_at) = 2020
    GROUP BY merchant_name
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+
| merchant_name   |   canceled_order_total |
|-----------------+------------------------|
| Saramakeup      |                      3 |
| Fernaz Cafe     |                      2 |
| Namshi          |                      2 |
| Theviolet store |                      2 |
| qatfat          |                      2 |
| Mioist1         |                      1 |
| Salla-Test      |                      1 |
| Elegant Look    |                      1 |
| Dar Lena        |                      1 |
| Coffee drop SA  |                      1 |
+-----------------+------------------------+

Total late fee amount collected by day
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, DATE(recorded_at) as date
    FROM tamara_staging.order_late_fee_infos
    GROUP BY DATE(recorded_at), currency
    ORDER BY DATE(recorded_at)
 result:
+-------------------------+------------+------------+
|   late_fee_total_amount | currency   | day        |
|-------------------------+------------+------------|
|                 25      | SAR        | 2020-08-07 |
|                 25      | SAR        | 2020-08-14 |
|                 25      | SAR        | 2020-09-17 |
|                 25      | SAR        | 2020-09-24 |
|                  8.9375 | SAR        | 2020-10-02 |
|                 25      | SAR        | 2020-10-18 |
|                 50      | SAR        | 2020-10-25 |
|                 25      | SAR        | 2020-10-26 |
|                 25      | SAR        | 2020-10-27 |
|                 25      | SAR        | 2020-11-02 |
|                 25      | SAR        | 2020-11-03 |
|                 25      | SAR        | 2020-11-04 |
|                 50      | SAR        | 2020-11-10 |
|                 75      | SAR        | 2020-11-11 |
|                 50      | SAR        | 2020-11-14 |
|                 25      | SAR        | 2020-11-15 |
|                150      | SAR        | 2020-11-17 |
|                 50      | SAR        | 2020-11-18 |
|                 63.6475 | SAR        | 2020-11-19 |
|                 75      | SAR        | 2020-11-20 |
|                150      | SAR        | 2020-11-21 |
|                125      | SAR        | 2020-11-22 |
|                 25      | SAR        | 2020-11-23 |
|                 88.995  | SAR        | 2020-11-25 |
|                100.5    | SAR        | 2020-11-26 |
|                 25      | SAR        | 2020-11-27 |
|                225      | SAR        | 2020-11-28 |
|                 25      | SAR        | 2020-11-29 |
|                 50      | SAR        | 2020-11-30 |
|                 50      | SAR        | 2020-12-01 |
|                150      | SAR        | 2020-12-02 |
|                 75      | SAR        | 2020-12-03 |
|                175      | SAR        | 2020-12-05 |
|                 79.5    | SAR        | 2020-12-06 |
|                125      | SAR        | 2020-12-07 |
|                150      | SAR        | 2020-12-08 |
|                175      | SAR        | 2020-12-09 |
|                 67.3125 | SAR        | 2020-12-10 |
|                 31.125  | SAR        | 2020-12-12 |
|                136      | SAR        | 2020-12-14 |
|                 25      | SAR        | 2020-12-15 |
|                 25      | SAR        | 2020-12-16 |
|                 25      | SAR        | 2020-12-17 |
|                147.15   | SAR        | 2020-12-18 |
|                 25      | SAR        | 2020-12-19 |
|                 75      | SAR        | 2020-12-21 |
|                 75      | SAR        | 2020-12-22 |
|                150      | SAR        | 2020-12-23 |
|                316.468  | SAR        | 2020-12-24 |
|                 25      | SAR        | 2020-12-26 |
|                 87.5    | SAR        | 2020-12-29 |
|                 25      | SAR        | 2020-12-31 |
|                 25      | SAR        | 2021-01-01 |
|                 25      | SAR        | 2021-01-02 |
|                101      | SAR        | 2021-01-03 |
|                 25      | SAR        | 2021-01-04 |
|                 28.1875 | SAR        | 2021-01-05 |
|                 50      | SAR        | 2021-01-06 |
|                110      | SAR        | 2021-01-07 |
|                223.805  | SAR        | 2021-01-09 |
|                 15.7525 | SAR        | 2021-01-11 |
|                 69      | SAR        | 2021-01-14 |
|                 25      | SAR        | 2021-01-15 |
|                 50      | SAR        | 2021-01-17 |
|                 66.8575 | SAR        | 2021-01-18 |
|                 25      | SAR        | 2021-01-19 |
|                 75      | SAR        | 2021-01-21 |
|                 95      | SAR        | 2021-01-22 |
|                174.75   | SAR        | 2021-01-24 |
|                 50      | SAR        | 2021-01-29 |
|                 50      | SAR        | 2021-02-01 |
|                 15.3925 | SAR        | 2021-02-02 |
|                 25      | SAR        | 2021-02-03 |
|                143.5    | SAR        | 2021-02-06 |
|                121.167  | SAR        | 2021-02-08 |
|                 50      | SAR        | 2021-02-13 |
|                 37.5    | SAR        | 2021-02-16 |
|                 15      | SAR        | 2021-02-18 |
|                 40      | SAR        | 2021-02-21 |
|                 54.82   | SAR        | 2021-02-23 |
|                 87.375  | SAR        | 2021-03-03 |
|                137.5    | SAR        | 2021-03-04 |
|                 25      | AED        | 2021-03-09 |
|                 25      | AED        | 2021-03-14 |
|                140      | SAR        | 2021-04-27 |
|                 37.6625 | SAR        | 2021-06-16 |
|               3430.7    | SAR        | 2021-08-31 |
+-------------------------+------------+------------+

Total late fee amount collected by month
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, MONTH(recorded_at) as month
    FROM tamara_staging.order_late_fee_infos
    GROUP BY MONTH(recorded_at), currency
    ORDER BY MONTH(recorded_at)
 result:
+-------------------------+------------+---------+
|   late_fee_total_amount | currency   |   month |
|-------------------------+------------+---------|
|               1234.35   | SAR        |       1 |
|                552.38   | SAR        |       2 |
|                 50      | AED        |       3 |
|                224.875  | SAR        |       3 |
|                140      | SAR        |       4 |
|                 37.6625 | SAR        |       6 |
|               3480.7    | SAR        |       8 |
|                 50      | SAR        |       9 |
|                133.938  | SAR        |      10 |
|               1428.14   | SAR        |      11 |
|               2215.06   | SAR        |      12 |
+-------------------------+------------+---------+

Total late fee amount collected by quarter
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, QUARTER(recorded_at) as quarter
    FROM tamara_staging.order_late_fee_infos
    GROUP BY QUARTER(recorded_at), currency
    ORDER BY QUARTER(recorded_at)
 result:
+-------------------------+------------+-----------+
|   late_fee_total_amount | currency   |   quarter |
|-------------------------+------------+-----------|
|                  50     | AED        |         1 |
|                2011.61  | SAR        |         1 |
|                 177.662 | SAR        |         2 |
|                3530.7   | SAR        |         3 |
|                3777.14  | SAR        |         4 |
+-------------------------+------------+-----------+

Total late fee amount collected by year
query: 
SELECT SUM(amount) AS late_fee_total_amount, currency, YEAR(recorded_at) as year
    FROM tamara_staging.order_late_fee_infos
    GROUP BY YEAR(recorded_at), currency
    ORDER BY YEAR(recorded_at)
 result:
+-------------------------+------------+--------+
|   late_fee_total_amount | currency   |   year |
|-------------------------+------------+--------|
|                 3877.14 | SAR        |   2020 |
|                   50    | AED        |   2021 |
|                 5619.97 | SAR        |   2021 |
+-------------------------+------------+--------+
Export database to file: output/tamara_staging.sql
mysqldump: [Warning] Using a password on the command line interface can be insecure.
Stopping the containers

```

## Output database
```bash
├── output
│   └── tamara_staging.sql
```bash