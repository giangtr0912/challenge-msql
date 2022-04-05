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
- Setup project structure + Docker + code linting + mypy
- Storage:
Input: MySQL
Output: Mysql
- Answer all critical business questions

### Good to have:
- Pipeline that support incremental update in real time
- Unit tests and integration tests for critical logic
- Architecture: Cloud native (K8s)
- Define CICD (github, gitlab,...)


## Solution

### Local Execution

Executes the application

```
sh run.sh
```
Run and check the application outcomes

```
docker-compose logs app 
```

The outcomes

```
Starting mydb ... done

	



	################################ 
	database connect:
	host = db
	user = root
	password = root
	port = 3306
	db = tamara
	################################ 




database %s create successful tamara
=================================================================================================== test session starts ===================================================================================================
platform linux -- Python 3.8.13, pytest-7.1.1, pluggy-1.0.0 -- /usr/local/bin/python3
cachedir: .pytest_cache
rootdir: /tamara
collected 3 items                                                                                                                                                                                                         

tests/test_database.py::test_order_items_table_exists PASSED                                                                                                                                                        [ 33%]
tests/test_database.py::test_late_fee_table_exists PASSED                                                                                                                                                           [ 66%]
tests/test_utils.py::TestUtils::test_db_write PASSED                                                                                                                                                                [100%]

==================================================================================================== 3 passed in 0.87s ====================================================================================================
Databases initialization successful
Begin insert data into order_item_main_infos
Successfully insert data into order_item_main_infos
Begin insert data into order_late_fee_infos
Successfully insert data into order_late_fee_infos
Begin insert data into order_merchant_infos
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
SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, DATE(created_at) AS date
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, DATE(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+--------------------------------------+-----------------+-----------------+------------+
| merchant_id                          | merchant_name   |   new_order_day | day        |
|--------------------------------------+-----------------+-----------------+------------|
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |             121 | 2020-10-20 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |              92 | 2020-10-15 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              58 | 2020-10-21 |
| a807eb01-dc22-4e9a-bf57-ee4b1a8fbfd5 | Wosof           |              56 | 2020-10-14 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |              48 | 2020-09-23 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              33 | 2020-10-22 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |              32 | 2020-11-04 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              32 | 2020-11-04 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |              30 | 2020-10-21 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              30 | 2020-11-01 |
+--------------------------------------+-----------------+-----------------+------------+

Top 10 merchants who have most new order value by month
query: 
SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, MONTH(created_at) AS month
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, MONTH(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+--------------------------------------+-----------------+-----------------+---------+
| merchant_id                          | merchant_name   |   new_order_day |   month |
|--------------------------------------+-----------------+-----------------+---------|
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |             352 |      10 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             209 |      10 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             160 |       9 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |             139 |      11 |
| ccb9027b-c3c5-4cb6-befb-c12d4c4d5049 | Namshi          |             112 |      11 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             103 |      11 |
| a807eb01-dc22-4e9a-bf57-ee4b1a8fbfd5 | Wosof           |              80 |      10 |
| ac0e9fa3-4077-4e58-8c02-f0e5ccbb8f83 | Mix store       |              80 |      10 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              75 |       9 |
| 36e56a72-b450-4f96-8529-f2745fd8e218 | Dar Lena        |              74 |      11 |
+--------------------------------------+-----------------+-----------------+---------+

Top 10 merchants who have most new order value by quarter
query: 
SELECT merchant_id, merchant_name, COUNT(event_name) AS new_order_total, QUARTER(created_at) AS quarter
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, QUARTER(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+--------------------------------------+-----------------+-----------------+-----------+
| merchant_id                          | merchant_name   |   new_order_day |   quarter |
|--------------------------------------+-----------------+-----------------+-----------|
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |             491 |         4 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             312 |         4 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             160 |         3 |
| ccb9027b-c3c5-4cb6-befb-c12d4c4d5049 | Namshi          |             141 |         4 |
| 36e56a72-b450-4f96-8529-f2745fd8e218 | Dar Lena        |             119 |         4 |
| 9365dee3-f572-4d97-91a6-5e5e2b716559 | PowerX          |              88 |         4 |
| ac0e9fa3-4077-4e58-8c02-f0e5ccbb8f83 | Mix store       |              80 |         4 |
| a807eb01-dc22-4e9a-bf57-ee4b1a8fbfd5 | Wosof           |              80 |         4 |
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |              75 |         3 |
| 99e6ed98-bf61-4fa0-96f1-b18d32ddb162 | Pets Shop       |              56 |         4 |
+--------------------------------------+-----------------+-----------------+-----------+

Top 10 merchants who have most new order value by year
query: 
SELECT merchant_id, merchant_name, COUNT(event_name) as new_order_total, YEAR(created_at) AS year
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) as t3
    WHERE event_name LIKE '%OrderWasCreated%'
    GROUP BY merchant_name, merchant_id, YEAR(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+--------------------------------------+-----------------+-----------------+--------+
| merchant_id                          | merchant_name   |   new_order_day |   year |
|--------------------------------------+-----------------+-----------------+--------|
| 3156e27d-72a2-4e4d-98b1-7e90ce8e6047 | Saramakeup      |             566 |   2020 |
| 80c897b8-0db2-467a-936b-a22d7f093667 | Glamour         |             472 |   2020 |
| ccb9027b-c3c5-4cb6-befb-c12d4c4d5049 | Namshi          |             141 |   2020 |
| 36e56a72-b450-4f96-8529-f2745fd8e218 | Dar Lena        |             119 |   2020 |
| 9365dee3-f572-4d97-91a6-5e5e2b716559 | PowerX          |              88 |   2020 |
| a807eb01-dc22-4e9a-bf57-ee4b1a8fbfd5 | Wosof           |              80 |   2020 |
| ac0e9fa3-4077-4e58-8c02-f0e5ccbb8f83 | Mix store       |              80 |   2020 |
| 99e6ed98-bf61-4fa0-96f1-b18d32ddb162 | Pets Shop       |              56 |   2020 |
| 4082e974-8fa8-44fe-8a18-d5d40d144af0 | Kttaan          |              53 |   2020 |
| 8a96844d-b1a4-49aa-83aa-397da3c1ceef | muscles world   |              51 |   2020 |
+--------------------------------------+-----------------+-----------------+--------+

Top 10 merchants who have most canceled order value by day
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total, DATE(created_at) AS date
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, DATE(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+------------+
| merchant_name   |   canceled_order_total | day        |
|-----------------+------------------------+------------|
| Saramakeup      |                      5 | 2020-09-30 |
| Saramakeup      |                      4 | 2020-10-21 |
| qatfat          |                      4 | 2020-12-10 |
| Fernaz Cafe     |                      4 | 2020-11-03 |
| Elegant Look    |                      3 | 2020-10-24 |
| Namshi          |                      3 | 2020-10-27 |
| Saramakeup      |                      2 | 2020-09-17 |
| Salla-Test      |                      2 | 2020-10-18 |
| Fernaz Cafe     |                      2 | 2020-10-18 |
| Namshi          |                      2 | 2021-03-14 |
+-----------------+------------------------+------------+

Top 10 merchants who have most canceled order value by month
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total, MONTH(created_at) AS month
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, MONTH(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+---------+
| merchant_name   |   canceled_order_total |   month |
|-----------------+------------------------+---------|
| Saramakeup      |                      7 |       9 |
| Saramakeup      |                      4 |      10 |
| qatfat          |                      4 |      12 |
| Fernaz Cafe     |                      4 |      11 |
| Elegant Look    |                      3 |      10 |
| Namshi          |                      3 |      10 |
| Salla-Test      |                      2 |      10 |
| Fernaz Cafe     |                      2 |      10 |
| Namshi          |                      2 |       3 |
| Mioist1         |                      1 |      10 |
+-----------------+------------------------+---------+

Top 10 merchants who have most canceled order value by quarter
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total, QUARTER(created_at) AS quarter
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, QUARTER(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+-----------+
| merchant_name   |   canceled_order_total |   quarter |
|-----------------+------------------------+-----------|
| Saramakeup      |                      7 |         3 |
| Fernaz Cafe     |                      6 |         4 |
| Saramakeup      |                      4 |         4 |
| qatfat          |                      4 |         4 |
| Elegant Look    |                      3 |         4 |
| Namshi          |                      3 |         4 |
| Salla-Test      |                      2 |         4 |
| Namshi          |                      2 |         1 |
| Theviolet store |                      2 |         4 |
| Mioist1         |                      1 |         4 |
+-----------------+------------------------+-----------+

Top 10 merchants who have most canceled order value by year
query: 
SELECT merchant_name, COUNT(event_name) AS canceled_order_total, YEAR(created_at) AS year
    FROM
    (SELECT * FROM tamara_staging.order_item_main_infos t1 NATURAL JOIN tamara_staging.order_merchant_infos t2 WHERE t1.order_id = t2.order_id) AS t3
    WHERE event_name LIKE '%OrderWasCanceled%'
    GROUP BY merchant_name, YEAR(created_at)
    ORDER BY COUNT(event_name) DESC
    LIMIT 10
 result:
+-----------------+------------------------+--------+
| merchant_name   |   canceled_order_total |   year |
|-----------------+------------------------+--------|
| Saramakeup      |                     11 |   2020 |
| Fernaz Cafe     |                      6 |   2020 |
| qatfat          |                      4 |   2020 |
| Elegant Look    |                      3 |   2020 |
| Namshi          |                      3 |   2020 |
| Salla-Test      |                      2 |   2020 |
| Namshi          |                      2 |   2021 |
| Theviolet store |                      2 |   2020 |
| Mioist1         |                      1 |   2020 |
| Dar Lena        |                      1 |   2020 |
+-----------------+------------------------+--------+

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
```

Stopping containers and cleaning

```
docker-compose down 
```