0. python3 -m pip install mysql-connector
0. pip freeze > requirements.txt
0. docker stop $(docker ps -aq) --> docker rm $(docker ps -aq) --> docker rmi $(docker images -q)
0. docker-compose up -d

1. docker-compose up (https://xuanthulab.net/lenh-docker-compose-tao-va-chay-cac-dich-vu-docker.html)
2. docker ps -a
3. docker exec -it de-coding-challenge-main_db_1 /bin/bash (https://www.youtube.com/watch?v=X8W5Xq9e2Os)
4. mysql -uroot -p -P3306 -h127.0.0.1 (de su dung voi mysql-workbench, https://linuxhint.com/installing_mysql_workbench_ubuntu/), chay mysql-workbench
3. select user,host from mysql.user;
4. Neu chua thay % ben canh root: update mysql.user set host='%' where user='root';
5. show databases;
6. USE tamara;
7. show tables;
8. select * from order_events;
9. select * from orders;


10.         cursor.execute(f"""SELECT DISTINCT query_id
                FROM wordcloud_count
                WHERE date = '{file_date}'""")
                @REM https://stackoverflow.com/questions/65931765/how-to-unit-test-a-method-that-contains-a-database-call-in-python
                @REM https://github.com/thisbejim/Pyrebase/blob/master/tests/test_database.py
                @REM https://stackoverflow.com/questions/28635671/using-sql-server-stored-procedures-from-python-pyodbc