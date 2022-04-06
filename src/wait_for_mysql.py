import getopt
import sys
import time

import MySQLdb

host = "db"
user = "root"
password = "root"
port = 3306
db = "tamara"

opts, args = getopt.getopt(sys.argv[1:], 'h:u:p:P:d:')

for opt, arg in opts:
    if opt in ("-h", "--db"):
        host = arg
    elif opt in ("-u", "--user"):
        user = arg
    elif opt in ("-p", "--password"):
        password = arg
    elif opt in ("-P", "--port"):
        port = int(arg)
    elif opt in ("-d", "--db"):
        db = arg

message = """
        \n\n\n
        ################################
        database connect:
        host = %s
        user = %s
        password = %s
        port = %s
        db = %s
        ################################ \n\n\n
""" % (host, user, password, port, db)

print(message)

while True:
    try:
        conn = MySQLdb.connect(host=host,
                               user=user,
                               passwd=password,
                               port=port)

        while True:
            cursor = conn.cursor()
            cursor.execute("show databases like '" + db + "'")
            result = cursor.fetchone()

            if result and len(result) > 0:
                print(f"database {db} create successful")
                break
            else:
                print(f"database {db} not created... waiting...")
                time.sleep(1)

            cursor.close()

        conn.close()
        break
    except Exception as e:
        print(f"MYSQL not responds.. waiting for mysql up: {e}")
        time.sleep(1)
