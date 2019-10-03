import pymysql
import sys
import optparse

EXIT_CODE = 1
INSERT_STMT = "INSERT INTO %s ('name','department','number') VALUES (%s,%s,%s)"

parser = optparse.OptionParser()

parser.add_option('-d', '--db', dest='db', help='Give the database name to be connected to')
parser.add_option('-t', '--table', dest='table', help='Give the table name to be operated on')

(options, args) = parser.parse_args()

if not options.db:
    parser.error("Please enter database name")
    sys.exit(EXIT_CODE)

if not options.table:
    parser.error('Please enter table name')
    sys.exit(EXIT_CODE)

db, table = options.db, options.table.upper()
conn = pymysql.connect(host='localhost', user='root', password='Admin@1234', db=db)

create_stmt = "CREATE TABLE IF NOT EXISTS %s(ID INT(5) NOT NULL AUTO_INCREMENT,NAME VARCHAR(255) NOT NULL, DEPARTMENT VARCHAR(255) NOT NULL, SALARY FLOAT(9,2) NOT NULL, PRIMARY KEY(ID));" % table

try:
    with conn.cursor() as cur:
        cur.execute(create_stmt)
    conn.commit()

    EXIT_CODE = 0

except Exception, e:
    print e

finally:
    sys.exit(int(EXIT_CODE))
