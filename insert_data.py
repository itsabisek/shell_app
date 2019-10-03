import pymysql
import optparse
import sys
import os
# from concurrent.futures import ThreadPoolExecutor, as_completed


def insert_data(connection, data):
    INSERT_STMT = "INSERT INTO %s (NAME,DEPARTMENT,SALARY) VALUES (\"%s\",\"%s\",%d);   " % (table.upper(), data[0], data[1], int(data[2]))
    print INSERT_STMT
    with connection.cursor() as cur:
        cur.execute(INSERT_STMT)


EXIT_CODE = 1

parser = optparse.OptionParser()

parser.add_option('-d', '--db', dest='db', help='Give the database name to be connected to')
parser.add_option('-t', '--table', dest='table', help='Give the table name to be operated on')
parser.add_option('-r', '--data', dest='data', help='Give the path the file from which data is to be populated')

(options, args) = parser.parse_args()

if not options.db:
    parser.error("Please enter database name")
    sys.exit(EXIT_CODE)

if not options.table:
    parser.error('Please enter table name')
    sys.exit(EXIT_CODE)

if not options.data:
    parser.error("Please enter path to data")
    sys.exit(EXIT_CODE)

db, table = options.db, options.table
data_path = os.path.join(os.path.abspath(os.curdir), options.data)
data = open(data_path, 'r').read()

conn = pymysql.connect(host='localhost', user='root', password='Admin@1234', db=db)

try:
    for line in data.split('\n')[:-1]:
        # print line
        insert_data(conn, line.split(','))

    EXIT_CODE = 0

except Exception, e:
    print e

finally:
    conn.commit()
    conn.close()

sys.exit(EXIT_CODE)
